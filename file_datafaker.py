# pylint: disable=E1101

# Project:        datafaker
# Creation Date:  2022-04-12
# Author:         Peter Ebel (peter.ebel@santander.de)
# Objective:      creation of fake data for Big Data Programming Workshop
#
# Modification Log:
# Version Date        Modified By	Modification Details
# 1.0.0   2022-04-12  Ebel          Initial creation of the script

import random
import io
from faker import Faker

log_data_generation = open('log_data_generation.txt', 'w+')

def log_data_generation_msg(message):
    log_data_generation.write(message)


def main():
    fake = Faker('de_DE')

    datepart = '2024-05-09'
    max_records = 10000
    record_number = 1

    # load cities and postal codes into a dict
    cities = {}
    with io.open('cities.csv', encoding='utf-8') as f:
        for line in f:
            (key, val) = line.split("\t")
            cities[key] = val
    f.close()

    # open output files
    customers = io.open('customers.csv', 'w+')
    balances = io.open('balances.csv', 'w+')

    # write table headers
    customers_header = 'record_number' + '|'+ 'entity_id' + '|' + 'customer_number' + '|' + 'valid_from_date' + '|' + 'valid_to_date' + '|' + 'gender_code' + '|' + 'last_name'  + '|' + 'first_name' + '|' + 'birth_date' + '|' + 'country_code' + '|' + 'postal_code' + '|' + 'city' + '|' + 'street' + '|' + 'data_date_part' + '\n'
    balances_header = 'record_number' + '|'+ 'entity_id' + '|' + 'customer_number' + '|' + 'instalment_amount' + '|' + 'term' + '|' + 'debt_amount'  + '|' + 'data_date_part' + '\n'
    customers.writelines(str(customers_header))
    balances.writelines(str(balances_header))

    for _ in range(max_records):

        # create customer fake data
        entity_id = random.choice(['3293', '3294', '3295', '3296']) if random.randint(1, 100) < 100 else '3297'
        customer_number = str(fake.random.randint(100000000, 999999999))
        valid_from_date = fake.date_between(start_date="-2190d", end_date="today").strftime('%Y-%m-%d')
        valid_to_date = fake.date_between(start_date="-730d", end_date="-30d").strftime('%Y-%m-%d') if random.randint(0,
                                                                                                                      10) == 1 else '9999-12-31'
        gender_code = 'M' if random.randint(0, 1) == 0 else 'F'
        first_name = fake.first_name_male() if gender_code == 'M' else fake.first_name_female()
        last_name = fake.last_name()
        birth_date = fake.date_of_birth(tzinfo=None, minimum_age=18, maximum_age=80).strftime('%Y-%m-%d')
        c = random.choice(list(cities.items()))
        postal_code = c[0]
        city = c[1].rstrip()
        street = fake.street_name()

        # compose and write customer record
        customer_record = str(
            record_number) + '|' + entity_id + '|' + customer_number + '|' + valid_from_date + '|' + valid_to_date + '|' + gender_code + '|' + last_name + '|' + first_name + '|' + birth_date + '|' + 'DE' + '|' + postal_code + '|' + city + '|' + street + '|' + datepart + '\n'
        customers.writelines(customer_record)

        # create balances fake data
        entity_id_balances = entity_id
        cn = random.randint(1, 100)
        if cn < 100:
            customer_number_balances = customer_number
        else:
            customer_number_balances = '000000000'
            log_data_generation_msg(
                'balances | record %s | referential integrity error, unknown customer number\n' % str(
                    record_number).rjust(len(str(max_records))))

        instalment_amount = round(random.uniform(100.00, 500.00), 2)
        term = random.randint(1, 48)
        rd = random.randint(1, 300)
        if rd < 298:
            residual_debt = round(instalment_amount * term, 2)
        elif rd == 298:
            residual_debt = round(instalment_amount * term, 2) + 1
            log_data_generation_msg(
                'balances | record %s | residual debt calculation is wrong\n' % str(record_number).rjust(
                    len(str(max_records))))
        elif rd == 299:
            instalment_amount = 10.00
            residual_debt = "%.2f" % round(instalment_amount * term, 2)
            log_data_generation_msg('balances | record %s | minValue violation: instalment amount < 100\n' % str(
                record_number).rjust(len(str(max_records))))
        else:
            term = 72
            residual_debt = "%.2f" % round(instalment_amount * term, 2)
            log_data_generation_msg(
                'balances | record %s | maxValue violation: term > 48\n' % str(record_number).rjust(
                    len(str(max_records))))

        balances_record = str(record_number) + '|' + entity_id_balances + '|' + customer_number_balances + '|' + str(
            "%.2f" % instalment_amount) + '|' + str(term) + '|' + str(residual_debt) + '|' + datepart + '\n'
        balances.writelines(balances_record)

        record_number = record_number + 1

    # close all open files
    customers.close()
    balances.close()
    log_data_generation.close()


if __name__ == '__main__':
    main()
