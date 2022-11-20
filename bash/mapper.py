
import sys
from datetime import datetime

# CONSTs
DATE_PICK_UP_IDX = 1
PULL_LOCATION_IDX = 7
PAYMENT_TYPE_IDX = 9
PASSENGER_COUNT_IDX = 3
PAYMENT_TYPE_CASH = 2


def prepare_variables(line):
    """
    Function to prepare needed variables
    :param line ( str )
    :return ( tuple )
    """
    values = line.split(",")

    date_pick_up = datetime.strptime(values[DATE_PICK_UP_IDX], "%Y-%m-%d %H:%M:%S")

    pull_location = values[PULL_LOCATION_IDX]
    payment_type = values[PAYMENT_TYPE_IDX]
    month = date_pick_up.month
    passenger_count = values[PASSENGER_COUNT_IDX]
    year = date_pick_up.year
    return year, month, payment_type, pull_location, passenger_count


def print_output(year, month, payment_type, pull_location, passenger_count):
    """ Function to print formatted output. """
    if month:
        if int(payment_type) == PAYMENT_TYPE_CASH:
            print(f'{year}:{month}:{pull_location}\t{passenger_count}')


if __name__ == '__main__':
    for line_num, line in enumerate(sys.stdin):
        if not (line and line.strip()) or 'VendorID' in line or line_num < 5:
            continue

        variables = prepare_variables(line)
        print_output(*variables)
