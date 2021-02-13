

import psycopg2
import psycopg2.extras

import faker
import random

def add_seller(connection):
    try:
        cursor = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

        fake = faker.Faker()
        seller_name = fake.name()
        seller_address = fake.address()
        seller_phone = fake.phone_number()

        cursor.execute('''
            INSERT INTO sellers(seller_name, seller_address, seller_phone)
            VALUES(%s, %s, %s)
            RETURNING seller_pid
        ''', (seller_name, seller_address, seller_phone))

        seller_pid = dict(cursor.fetchone())['seller_pid']

        items_count = random.randint(1, 100)
        for item_index in range(0, items_count):
            item_name = fake.text()
            item_description = fake.text()
            item_price = random.randint(0, 100) + float(random.randint(0, 100) / 100.0);

            cursor.execute('''
                INSERT INTO items(item_name, item_description, item_price, seller_pid)
                VALUES(%s, %s, %s, %s)
                RETURNING item_id
            ''', (item_name, item_description, item_price, seller_pid))

            item_id = dict(cursor.fetchone())['item_id']

    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        cursor.close()
        print("[ ERROR ] Adding seller: %s" % (
            seller_name,
        ))
        print(str(error))
        return

    connection.commit()
    cursor.close()
    print("[ OK ] Adding seller: %s" % (
        seller_name,
    ))

def connect():
    print('Connecting to the PostgreSQL database...')
    connection = psycopg2.connect(
        host = "192.168.122.51",
        database = "unixway1",
        user = "unixway1user",
        password = "password1"
    )

    for index in range(0, 1000):
        add_seller(
            connection = connection
        )


if __name__ == '__main__':
    connect()

# Add id, add timestamp
