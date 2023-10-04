# pylint: disable=E1101

# Project:        datafaker_postgres
# Creation Date:  2023-08-11
# Author:         Peter Ebel (peter.ebel@santander.de)
# Objective:      creation of fake data records in PostreSQL
#
# Modification Log:
# Version Date        Modified By	Modification Details
# 1.0.0   2023-08-11  Ebel          Initial creation of the script

import random
import io
import time
import psycopg2
from threading import Timer
from faker import Faker

def get_max_number(database, table, column):
     stmt = "SELECT MAX({column}) from {database}.{table};".format(column=column, database=database, table=table)
     cursor.execute(stmt)
     return cursor.fetchone()[0]

# connection to the database
def connect_to_postgres():

    global connection
    global cursor

    db_params = {
        'host': 'localhost',
        'database': 'galeria_anatomica',
        'user': 'peter',
        'password': 'guiltyspark',
    }

    # establish a connection to the database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        create_records('shop', 'customers', 10, 15, '2023-08-11')

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_records(database, table, max_records, delay, date_partition):
    fake = Faker('de_DE')
    record_number = get_max_number(database, table, 'record_number')
    if record_number is not None:
        record_number = record_number + 1
    else:
        record_number = 1    

    cities = {}
    with io.open('cities.csv', encoding='utf-8') as f:
        for line in f:
            (key, val) = line.split("\t")
            cities[key] = val
    f.close()

    for _ in range(max_records):
        time.sleep(delay)
        c = random.choice(list(cities.items()))
        gc = 'M' if random.randint(0, 1) == 0 else 'F'
        mydict = {
            'record_number': record_number,
            'entity_id': random.choice(['3293', '3294', '3295', '3296']) if random.randint(1, 100) < 100 else '3297',
            'customer_number': str(fake.random.randint(100000000, 999999999)),
            'valid_from_date': fake.date_between(start_date="-2190d", end_date="today").strftime('%Y-%m-%d'),
            'valid_to_date': fake.date_between(start_date="-730d", end_date="-30d").strftime('%Y-%m-%d') if random.randint(0,10) == 1 else '9999-12-31',
            'gender_code': gc,
            'first_name': fake.first_name_male() if gc == 'M' else fake.first_name_female(),
            'last_name': fake.last_name(),
            'birth_date': fake.date_of_birth(tzinfo=None, minimum_age=18, maximum_age=80).strftime('%Y-%m-%d'),
            'country_code': 'DE',
            'postal_code': c[0],
            'city': c[1].rstrip(),
            'street': fake.street_name(),
            'data_date_part': date_partition
        }
        record_number = record_number + 1

        # compose and write customer record
        placeholder = ", ".join(["%s"] * len(mydict))
        stmt = "INSERT INTO {database}.{table} ({columns}) VALUES ({values});".format(database=database, table=table, columns=", ".join(mydict.keys()), values=placeholder)
        cursor.execute(stmt, list(mydict.values()))
        connection.commit()

def main():
    connect_to_postgres()

if __name__ == '__main__':
    main()
