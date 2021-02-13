

import psycopg2
import psycopg2.extras

import faker
import random

def add_user(connection, user_name, user_email, user_balance):
    try:
        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

        cursor.execute('''
            INSERT INTO users(user_name, user_email)
            VALUES(%s, %s)
            RETURNING user_pid, user_id
        ''', (user_name, user_email))

        user = dict(cursor.fetchone())
        user_pid = user['user_pid']
        user_id = user['user_id']

        cursor.execute('''
            INSERT INTO balances(user_pid, balance_total, balance_diff)
            VALUES(%s, %s, %s)
            RETURNING balance_pid
        ''', (user_pid, user_balance, 0))

    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        cursor.close()
        print("[ ERROR ] Adding user: %s, email: %s, balance: %s" % (
            user_name,
            user_email,
            user_balance
        ))
        print(str(error))
        return

    connection.commit()
    cursor.close()
    print("[ OK ] Adding user: %s, email: %s, balance: %s, id: %s" % (
        user_name,
        user_email,
        user_balance,
        user_id
    ))

def connect():
    print('Connecting to the PostgreSQL database...')
    connection = psycopg2.connect(
        host = "192.168.122.51",
        database = "unixway1",
        user = "unixway1user",
        password = "password1"
    )

    fake = faker.Faker()
    for index in range(0, 10000):
        seed = random.randint(1, 1000000000000000)

        faker.Faker.seed(seed)
        user_name = fake.name()

        faker.Faker.seed(seed)
        user_email = fake.email()

        user_balance = random.randint(1, 1000)

        add_user(
            connection = connection,
            user_name = user_name,
            user_email = user_email,
            user_balance = user_balance
        )


if __name__ == '__main__':
    connect()

# Add id, add timestamp
