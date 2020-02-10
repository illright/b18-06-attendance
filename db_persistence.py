from collections import defaultdict
import pickle
from urllib.parse import urlparse

import psycopg2
from telegram.ext import BasePersistence


class DBPersistence(BasePersistence):
    def __init__(self, db_url):
        super().__init__()

        self.store_bot_data = False
        self.store_chat_data = True
        self.store_user_data = False

        parsed_url = urlparse(db_url)
        self.connection = psycopg2.connect(
            database=parsed_url.path[1:],
            user=parsed_url.username,
            password=parsed_url.password,
            host=parsed_url.hostname,
            port=parsed_url.port,
        )
        c = self.connection.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS chat_data (
                chat_id integer,
                headers bytea,
                attendance bytea,
                users bytea
            )
        ''')
        self.connection.commit()
        c.close()

    def get_chat_data(self):
        c = self.connection.cursor()
        c.execute('''SELECT chat_id, headers, attendance, users FROM chat_data''')
        data = defaultdict(dict)
        for row in c.fetchall():
            data[row[0]] = {'headers': pickle.loads(row[1]),
                            'attendance': pickle.loads(row[2]),
                            'users': pickle.loads(row[3])}
        return data

    def update_chat_data(self, chat_id, data):
        c = self.connection.cursor()
        if 'headers' not in data:
            c.execute('DELETE FROM chat_data')
            self.connection.commit()
            return

        c.execute('''SELECT chat_id, headers, attendance, users
                     FROM chat_data WHERE chat_id = %s''', (chat_id,))
        full_data = c.fetchone()
        if full_data is not None:
            full_data = list(full_data)
            full_data[1] = pickle.dumps(data['headers'])
            full_data[2] = pickle.dumps(data['attendance'])
            full_data[3] = pickle.dumps(data['users'])
            full_data.append(full_data.pop(0))
            c.execute('''UPDATE chat_data
                         SET headers = %s, attendance = %s, users = %s
                         WHERE chat_id = %s''', full_data)
        else:
            full_data = [chat_id, pickle.dumps(data['headers']),
                                  pickle.dumps(data['attendance']),
                                  pickle.dumps(data['users'])]
            c.execute('''INSERT INTO chat_data (chat_id, headers, attendance, users)
                         VALUES (%s, %s, %s, %s)''', full_data)
        self.connection.commit()
        c.close()
