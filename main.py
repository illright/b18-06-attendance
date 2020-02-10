'''DoE Attendance marking bot.'''

import csv
import io
import json
import logging
import os
from datetime import datetime, time, timezone, timedelta

from telegram import InlineKeyboardMarkup, InlineKeyboardButton, ParseMode
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, PicklePersistence

from db_persistence import DBPersistence

telegram_key = os.getenv('TELEGRAM_API_KEY')


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
    if isinstance(item, str):
        return item

    date = datetime.strptime(item[0], '%Y-%m-%d')
    class_ = schedule[date.weekday()][item[1]]
    return '\n'.join((date.strftime('%d.%m'), class_['name'], class_['type']))


def set_up_notifications(update, context):
    '''Set up notifications for this chat, recording the jobs for further teardown.'''
    logger.info('Setting up notifications')
    if update.message.chat_id not in jobs:
        jobs[update.message.chat_id] = []
    else:
        logger.info('Previous jobs found, cancelling them first')
        for job in jobs[update.message.chat_id]:
            job.schedule_removal()
        jobs[update.message.chat_id].clear()

    if 'users' not in context.chat_data:
        context.chat_data['users'] = {}

    if 'attendance' not in context.chat_data:
        context.chat_data['attendance'] = {}

    if 'headers' not in context.chat_data:
        context.chat_data['headers'] = ['Student']

    for slot_idx, slot in enumerate(timeslots):
        job = updater.job_queue.run_daily(notify_for_class,
                                          time=slot,
                                          days=workdays,
                                          context={'slot_idx': slot_idx,
                                                   'chat_id': update.message.chat_id,
                                                   'chat_data': context.chat_data})
        jobs[update.message.chat_id].append(job)

    update.message.reply_text('Notifications set up successfully!\nFor each notification, click '
                              'the button once to mark yourself present and twice to unmark '
                              'your presence.')


def tear_down_notifications(update, context):
    '''Remove all active jobs for this chat.'''
    logger.info('Tearing down notifications')
    export_data(update, context)
    if update.message.chat_id not in jobs:
        return

    for job in jobs[update.message.chat_id]:
        job.schedule_removal()

    context.chat_data.pop('users')
    context.chat_data.pop('attendance')
    context.chat_data.pop('headers')

    update.message.reply_text('Removed notifcations and dropped all statistics.')


def notify_for_class(context):
    '''A periodic job set up for each class to send a notification.'''
    job = context.job
    bot = context.bot
    weekday = datetime.now().weekday()
    logger.info(f"Running for slot {job.context['slot_idx']} for weekday {weekday}")

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
    bot.send_message(job.context['chat_id'],
                     notification.format(**class_),
                     parse_mode=ParseMode.HTML,
                     reply_markup=keyboard)

    job.context['chat_data']['headers'].append((date[:4+3+3], job.context["slot_idx"]))


def mark_attendance(update, context):
    '''Handler for when people actually press the attendance button.'''
    if not context.chat_data:
        logger.info('An old message triggered, ignoring')
        return

    date, slot = update.callback_query.data.split(',')
    slot = int(slot)
    logger.info(f'{update.callback_query.from_user.full_name} has marked their attendance '
                f'status on slot {slot} of {date}')

    class_ = schedule[datetime.fromisoformat(date).weekday()][slot]

    user_id = update.callback_query.from_user.id
    if user_id not in context.chat_data['users']:
        context.chat_data['users'][user_id] = update.callback_query.from_user.full_name
        context.chat_data['attendance'][user_id] = {}

    if context.chat_data['attendance'][user_id].get((date[:4+3+3], slot), False):
        context.chat_data['attendance'][user_id][date[:4+3+3], slot] = 0
    else:
        context.chat_data['attendance'][user_id][date[:4+3+3], slot] = 1

    update.callback_query.answer()
    attendees = sum(context.chat_data['attendance'][user].get((date[:4+3+3], slot), 0)
                    for user in context.chat_data['attendance'])
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

def export_data(update, context):
    '''Return the statistics as a CSV file.'''
    logger.info('Exporting statistics')
    if not context.chat_data.get('attendance'):
        update.message.reply_text('No attendance statistics yet.')
        return

    file = io.StringIO()
    writer = csv.writer(file)
    writer.writerow(map(get_cell_text, context.chat_data['headers']))
    for student in sorted(context.chat_data['attendance'],
                          key=lambda x: context.chat_data['users'][x]):
        row = []
        for cell in context.chat_data['headers']:
            if cell == 'Student':
                row.append(context.chat_data['users'][student])
            else:
                row.append(context.chat_data['attendance'][student].get(cell, 0))
        writer.writerow(row)
    binary_file = io.BytesIO(file.getvalue().encode())
    update.message.reply_document(binary_file, filename='B18-06 Attendance.csv')


def error_handler(bot, update, context):
    logger.error(context)


# pylint: disable=invalid-name
dbp = DBPersistence(db_url=os.getenv('DATABASE_URL'))
updater = Updater(telegram_key,
                  use_context=True,
                  persistence=dbp)
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

dp.add_handler(CommandHandler('start', set_up_notifications))
dp.add_handler(CommandHandler('stop', tear_down_notifications))
dp.add_handler(CallbackQueryHandler(mark_attendance))
dp.add_handler(CommandHandler('export', export_data))

dp.add_error_handler(error_handler)

updater.start_polling()
updater.idle()
