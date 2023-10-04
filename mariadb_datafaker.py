# pylint: disable=E1101

# Project:        db_datafaker
# Creation Date:  2022-04-13
# Author:         Peter Ebel (peter.ebel@santander.de)
# Objective:      creation of fake data records in MySQL for CDC
#
# Modification Log:
# Version Date        Modified By	Modification Details
# 1.0.0   2022-04-13  Ebel          Initial creation of the script

import random
import io
import time
import mysql.connector
from faker import Faker

# create MySQL database connector
mydb = mysql.connector.connect(
    host="localhost",
    user="spark",
    password="***",
    database="shop"
)
mycursor = mydb.cursor()

log_data_generation = open('log_data_generation.txt', 'w+')

def log_data_generation_msg(message):
    log_data_generation.write(message)


def get_max_number(database, table, column):
     stmt = "SELECT MAX({column}) from {database}.{table};".format(column=column, database=database, table=table)
     mycursor.execute(stmt)
     return mycursor.fetchone()[0]

def create_records(database, table, max_records, date_partition):
    fake = Faker('de_DE')
    datepart = '2022-04-08'
    record_number = get_max_number('shop', 'customers', 'record_number')
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
            'postal_code': c[0],
            'city': c[1].rstrip(),
            'street': fake.street_name()
        }
        record_number = record_number + 1

        # compose and write customer record
        placeholder = ", ".join(["%s"] * len(mydict))
        stmt = "INSERT INTO {database}.{table} ({columns}) VALUES ({values});".format(database=database, table=table, columns=", ".join(mydict.keys()), values=placeholder)
        mycursor.execute(stmt, list(mydict.values()))
        mydb.commit()
        print(record_number)

        time.sleep(30)


def main():
    create_records('shop', 'customers', 1000, '2022-04-13')


if __name__ == '__main__':
    main()
