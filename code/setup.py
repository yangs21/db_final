import psycopg2
import os
import csv
import threading

path = os.path.dirname(os.path.realpath(__file__))
progress = None
progress_timer_thread = None
cutoff_date = 1990

def setup_schema(connection_string):
    print("Loading data")
    conn = psycopg2.connect(connection_string)
    print ("Connection established")
    cursor = conn.cursor()
    print("Cursor created")
    with open(os.path.join(path, 'schema.sql'), 'r') as schema:
        setup_queries = schema.read()
        cursor.execute(setup_queries)
        conn.commit()

def company_information_query(line):
    return ("INSERT INTO company_information VALUES("\
                "%(ticker)s, "\
                "%(exchange)s, "\
                "%(name)s, "\
                "%(sector)s, "\
                "%(industry)s);",
            {
                'ticker': line[0],\
                'exchange': line[1],\
                'name': line[2],\
                'sector': line[3],\
                'industry': line[4]
            })

def historical_stock_prices_query(line):
    dateTotal = line[7]
    separatedDates = dateTotal.split('-')
    year = separatedDates[0]
    if int(year) > cutoff_date:
        return None
    return ("INSERT INTO historical_stock_prices VALUES("\
                "%(ticker)s, "\
                "%(open)s, "\
                "%(close)s, "\
                "%(adj_close)s, "\
                "%(low)s, "\
                "%(high)s, "\
                "%(volume)s, "\
                "%(trade_date)s);",
            {
                'ticker': line[0],\
                'open': line[1],\
                'close': line[2],\
                'adj_close': line[3],\
                'low': line[4],\
                'high': line[5],\
                'volume': line[6],\
                'trade_date': line[7]
            })

def attacks_query(line):
    year = line[1].zfill(4)
    month = line[2].zfill(2)
    day = line[3].zfill(2)
    date = '-'.join([year, month, day])
    if year == '0000' or month == '00' or day == '00':
        date = None
    summary = line[18]
    if len(summary) > 511:
        summary = None
    success = True
    if int(year) > cutoff_date:
        return None
    return ("INSERT INTO attacks VALUES("\
                "%(id)s, "\
                "%(date)s, "\
                "%(summary)s);",
            {
                'id': line[0],\
                'date':  date,\
                'summary': summary
            })

def attack_location_query(line):
    year = line[1].zfill(4)
    if int(year) > 1982:
        return None
    return ("INSERT INTO attack_location VALUES("\
                "%(id)s, "\
                "%(country)s, "\
                "%(region)s, "\
                "%(provstate)s, "\
                "%(city)s);",
            {
                'id': line[0],\
                'country': line[8],\
                'region': line[10],\
                'provstate': line[11],\
                'city': line[12]
            })

def attack_data_query(line):
    year = line[1].zfill(4)
    if int(year) > cutoff_date:
        return None
    number_killed = line[98]
    if number_killed == '':
        number_killed = None
    return ("INSERT INTO attack_data VALUES("\
                "%(id)s, "\
                "%(extended)s, "\
                "%(multiple)s, "\
                "%(success)s, "\
                "%(suicide)s, "\
                "%(attack_type)s, "\
                "%(target_type)s, "\
                "%(target_nationality)s, "\
                "%(number_killed)s);",
            {
                'id': line[0],\
                'extended': line[5],\
                'multiple': line[25],\
                'success': line[26],\
                'suicide': line[27],\
                'attack_type': line[29],\
                'target_type': line[35],\
                'target_nationality': line[41],\
                'number_killed': number_killed
            })

def print_progress():
    global progress
    global progress_timer_thread
    if progress is not None:
        print("{} rows inserted".format(progress))
    progress_timer_thread = threading.Timer(5.0, print_progress)
    progress_timer_thread.start()

def load_file(connection_string, query_factories, filename, encoding='utf_8'):
    global progress
    global progress_timer_thread
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    with open(os.path.join(path, 'datasets', filename), 'r', encoding=encoding) as file:
        reader = csv.reader(file)
        skip_first_line = True
        print_progress()
        for idx, line in enumerate(reader):
            progress = idx
            if skip_first_line:
                skip_first_line = False
                continue
            for factory in query_factories:
                query = factory(line)
                if query == None:
                    break
                cursor.execute(query[0], query[1])
        conn.commit()
        if progress_timer_thread is not None:
            progress_timer_thread.cancel()