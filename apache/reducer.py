#!/usr/bin/env python3
import sys

current_year_month_location = None
current_count = 0
year_month_location = None

for line in sys.stdin:

    line = line.strip()

    year_month_location, count = line.split('\t', 1)

    try:
        count = int(count)
    except ValueError:

        continue

    if current_year_month_location == year_month_location:
        current_count += count
    else:
        if current_year_month_location:
            # write result to STDOUT
            try:
                st = current_year_month_location.split(':')
                print('%s\t%s\t%s\t%s' % (st[0], st[1], st[2], current_count))
            except AttributeError:
                pass
        current_count = count
        current_year_month_location = year_month_location

# do not forget to output the last year_month_location if needed!
if current_year_month_location == year_month_location:
    try:
        st = current_year_month_location.split(':')
        print('%s\t%s\t%s\t%s' % (st[0], st[1], st[2], current_count))
    except AttributeError:
        pass
