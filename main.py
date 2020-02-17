'''DoE Attendance marking bot.'''

import csv
import io
import json
import logging
import os
from urllib.parse import urlparse
from datetime import datetime, time, timezone, timedelta

from pymongo import MongoClient
from telegram import InlineKeyboardMarkup, InlineKeyboardButton, ParseMode
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler

db_uri = os.getenv('MONGODB_URI') + '?retryWrites=false'
mongo = MongoClient(db_uri)
db = mongo[urlparse(db_uri).path[1:]]

# Enable logging
# pylint: disable=logging-format-interpolation
logging.basicConfig(format='[%(asctime)s] [%(levelname)s] [bot]\n%(message)s',
                    datefmt='%d-%m %H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)
notification = '<b>{name}</b> – {type}\nRoom {room}'
jobs = {}


def get_cell_text(item):
    '''Transform a header element into the cell's text.'''
    if item == 'Student':
        return item

    date, slot_idx = item.split('S')
    date = datetime.strptime(date, '%Y-%m-%d')
    class_ = schedule[date.weekday()][int(slot_idx)]
    return '\n'.join((date.strftime('%d.%m'),
                      class_['name'],
                      class_['type']))


def trigger_setup(update, context):
    '''Set up notifications for this chat, recording the jobs for further teardown.'''
    logger.info(f'Setting up notifications for {update.message.chat_id}')
    set_up_notifications(str(update.message.chat_id))

    context.bot.send_message(update.message.chat_id,
                             'Notifications set up successfully!\n'
                             'If you no longer want to receive notifications in this chat, '
                             'use the /stop command.\n'
                             '<b>Warning</b>: it will export and reset the statistics.',
                             parse_mode=ParseMode.HTML)


def set_up_notifications(chat_id: str):
    '''Set up periodic jobs to send notifications.'''
    if chat_id not in jobs:
        jobs[chat_id] = []
    else:
        logger.info('Previous jobs found, cancelling them first')
        for job in jobs[chat_id]:
            job.schedule_removal()
        jobs[chat_id].clear()

    chat = db.chats.find_one({'id': chat_id})
    if chat is None:
        db.chats.insert_one({
            'id': chat_id,
            'users': {},
            'headers': ['Student'],
            'attendance': {},
        })

    for slot_idx, slot in enumerate(timeslots):
        job = updater.job_queue.run_daily(notify_for_class,
                                          time=slot,
                                          days=workdays,
                                          context={'slot_idx': slot_idx,
                                                   'chat_id': chat_id})
        jobs[chat_id].append(job)


def tear_down_notifications(update, context):
    '''Remove all active jobs for this chat.'''
    logger.info('Tearing down notifications')
    export_data(update, context)

    chat_id = str(update.message.chat_id)
    if chat_id not in jobs:
        return

    for job in jobs[chat_id]:
        job.schedule_removal()
    jobs.pop(chat_id)

    db.chats.delete_one({'id': chat_id})
    context.bot.send_message(update.message.chat_id,
                             'Removed notifcations and dropped all statistics.')


def notify_for_class(context):
    '''A periodic job set up for each class to send a notification.'''
    job = context.job
    bot = context.bot
    weekday = datetime.now().weekday()
    logger.info(f"Running for slot {job.context['slot_idx']} for weekday {weekday}")

    id_query = {'id': job.context['chat_id']}
    chat = db.chats.find_one(id_query)

    class_ = schedule[weekday][job.context["slot_idx"]]
    if class_ is None:
        logger.info('No class this time, skipping')
        return

    date = datetime.combine(datetime.now(), timeslots[job.context["slot_idx"]]).isoformat()
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(
            'I attended',
            callback_data=f'{date},{job.context["slot_idx"]}'
        )]]
    )
    bot.send_message(int(job.context['chat_id']),
                     notification.format(**class_),
                     parse_mode=ParseMode.HTML,
                     reply_markup=keyboard)

    chat['headers'].append(f'{date[:4+3+3]}S{job.context["slot_idx"]}')
    db.chats.update_one(id_query, {'$set': {'headers': chat['headers']}})


def mark_attendance(update, context):
    '''Handler for when people actually press the attendance button.'''
    id_query = {'id': str(update.effective_chat.id)}
    chat = db.chats.find_one(id_query)

    if not chat:
        logger.info('An old message triggered, ignoring')
        return

    date, slot = update.callback_query.data.split(',')
    slot = int(slot)
    logger.info(f'{update.callback_query.from_user.full_name} has marked their attendance '
                f'status on slot {slot} of {date}')

    class_ = schedule[datetime.fromisoformat(date).weekday()][slot]

    modifications = {}
    user_id = str(update.callback_query.from_user.id)
    if user_id not in chat['users']:
        chat['users'][user_id] = update.callback_query.from_user.full_name
        chat['attendance'][user_id] = {}
        modifications['users'] = chat['users']
        modifications['attendance'] = chat['attendance']

    class_id = f'{date[:4+3+3]}S{slot}'
    if chat['attendance'][user_id].get(class_id, False):
        chat['attendance'][user_id][class_id] = 0
        update.callback_query.answer('You have unmarked your presence. '
                                     'Press again to mark it back.')
    else:
        chat['attendance'][user_id][class_id] = 1
        update.callback_query.answer('You have marked your presence. Press again to unmark it.')
    modifications['attendance'] = chat['attendance']

    attendees = sum(chat['attendance'][user].get(class_id, 0)
                    for user in chat['attendance'])
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(
            'I attended',
            callback_data=f'{date},{slot}'
        )]]
    )
    if attendees:
        message = notification.format(**class_) + f'\n\nAttendees: {attendees}'
    else:
        message = notification.format(**class_)

    update.effective_message.edit_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard
    )
    db.chats.update_one(id_query, {'$set': modifications})


def export_data(update, context):
    '''Return the statistics as a CSV file.'''
    logger.info('Exporting statistics')
    chat = db.chats.find_one({'id': str(update.message.chat_id)})
    if not chat or not chat.get('attendance'):
        update.message.reply_text('No attendance statistics yet.')
        return

    file = io.StringIO()
    writer = csv.writer(file)
    writer.writerow(map(get_cell_text, chat['headers']))
    for student in sorted(chat['attendance'], key=lambda x: chat['users'][x]):
        row = []
        for cell in chat['headers']:
            if cell == 'Student':
                row.append(chat['users'][student])
            else:
                row.append(chat['attendance'][student].get(cell, 0))
        writer.writerow(row)
    binary_file = io.BytesIO(file.getvalue().encode())
    context.bot.send_document(update.message.chat_id, binary_file, filename='B18-06 Attendance.csv')


def error_handler(update, context):  # pylint: disable=unused-argument
    '''Log any exceptions that occur.'''
    logger.exception(context.error)


def recover_notifications():
    for chat in db.chats.find():
        logger.info(f'Restoring notifications for {chat["id"]}')
        set_up_notifications(chat['id'])


# pylint: disable=invalid-name
updater = Updater(os.getenv('TELEGRAM_API_KEY'), use_context=True)
dp = updater.dispatcher
schedule = json.load(open('schedule.json'))

kazan_tz = timezone(timedelta(hours=3))
workdays = tuple(range(len(schedule)))
timeslots = [
    # pylint: disable=bad-whitespace
    time(hour=9,  minute=00, tzinfo=kazan_tz),
    time(hour=10, minute=35, tzinfo=kazan_tz),
    time(hour=12, minute=10, tzinfo=kazan_tz),
    time(hour=14, minute=10, tzinfo=kazan_tz),
    time(hour=15, minute=45, tzinfo=kazan_tz),
    time(hour=17, minute=20, tzinfo=kazan_tz),
]

timeslots = [
    # pylint: disable=bad-whitespace
    time(hour=12, minute=2, second=0, tzinfo=kazan_tz),
    time(hour=12, minute=2, second=1, tzinfo=kazan_tz),
    time(hour=12, minute=2, second=2, tzinfo=kazan_tz),
    time(hour=12, minute=2, second=3, tzinfo=kazan_tz),
    time(hour=12, minute=2, second=4, tzinfo=kazan_tz),
    time(hour=12, minute=2, second=5, tzinfo=kazan_tz),
]

dp.add_handler(CommandHandler('start', trigger_setup))
dp.add_handler(CommandHandler('stop', tear_down_notifications))
dp.add_handler(CallbackQueryHandler(mark_attendance))
dp.add_handler(CommandHandler('export', export_data))

dp.add_error_handler(error_handler)

updater.start_polling()
recover_notifications()
updater.idle()
