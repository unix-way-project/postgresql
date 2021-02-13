

import psycopg2
import psycopg2.extras

import faker
import random

def add_order(connection):
    try:
        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

        # Choose random user
        cursor.execute('''
            SELECT user_pid
            FROM users
            ORDER BY random() DESC
            LIMIT 1;
        ''')

        user = dict(cursor.fetchone())
        user_pid = user['user_pid']

        # Choose random item
        cursor.execute('''
            SELECT item_pid, item_price
            FROM items
            ORDER BY random() DESC
            LIMIT 1;
        ''')

        item = dict(cursor.fetchone())
        item_pid = item['item_pid']
        item_price = item['item_price']

        cursor.execute('''
            INSERT INTO orders(user_pid, item_pid, order_price)
            VALUES(%s, %s, %s)
            RETURNING order_pid
        ''', (user_pid, item_pid, item_price))

        order = dict(cursor.fetchone())
        order_pid = order['order_pid']

        cursor.execute('''
            UPDATE users
            SET user_balance = user_balance - %s
            WHERE user_pid = %s
        ''', (item_price, user_pid))

    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        cursor.close()
        print("[ ERROR ] User: %s failed to order item %s for price: %s" % (
            user_pid,
            item_pid,
            item_price
        ))
        print(str(error))
        return

    connection.commit()
    cursor.close()
    print("[ ORDER ] User: %s ordered item %s for price: %s, order number: %s" % (
        user_pid,
        item_pid,
        item_price,
        order_pid
    ))

def connect():
    print('Connecting to the PostgreSQL database...')
    connection = psycopg2.connect(
        host = "192.168.122.51",
        database = "unixway1",
        user = "unixway1user",
        password = "password1"
    )

    for index in range(0, 100000):
        add_order(
            connection = connection
        )


if __name__ == '__main__':
    connect()
