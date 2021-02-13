

import psycopg2
import psycopg2.extras

import faker
import random

def get_users(connection):
    try:
        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

        # Choose random user
        cursor.execute('''
            SELECT user_pid
            FROM users
            ORDER BY random() DESC
        ''')

        rows = cursor.fetchall()
        users = [
            dict(user)
            for user in rows
        ]

    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        cursor.close()
        print(str(error))
        return

    connection.commit()
    cursor.close()

    return users

def get_items(connection):
    try:
        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

        # Choose random user
        cursor.execute('''
            SELECT item_pid, item_price
            FROM items
            ORDER BY random() DESC
        ''')

        rows = cursor.fetchall()
        items = [
            dict(item)
            for item in rows
        ]

    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        cursor.close()
        print(str(error))
        return

    connection.commit()
    cursor.close()

    return items

def add_order(connection, user, item):
    try:
        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

        user_pid = user['user_pid']

        item_pid = item['item_pid']
        item_price = item['item_price']

        # Get user last balance
        cursor.execute('''
            SELECT balance_total
            FROM balances
            WHERE user_pid = %s
            ORDER BY balance_registration DESC
            LIMIT 1;
        ''', (user_pid,))

        balance = dict(cursor.fetchone())
        balance_total = balance['balance_total'] # This is User Balance

        cursor.execute('''
            INSERT INTO orders(user_pid, item_pid, order_price)
            VALUES(%s, %s, %s)
            RETURNING order_pid
        ''', (user_pid, item_pid, item_price))

        order = dict(cursor.fetchone())
        order_pid = order['order_pid']

        cursor.execute('''
            INSERT INTO balances(user_pid, balance_total, balance_diff)
            VALUES(%s, %s, %s)
        ''', (user_pid, balance_total - item_price, - item_price))

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

    print("Fetching user list from database...")
    users = get_users(connection)
    print("Got %s users" % len(users))

    print("Fetching items list from database...")
    items = get_items(connection)
    print("Got %s items" % len(items))

    for index in range(0, 1500000):
        user = random.choice(users)
        item = random.choice(items)
        add_order(
            connection = connection,
            user = user,
            item = item
        )


if __name__ == '__main__':
    connect()
