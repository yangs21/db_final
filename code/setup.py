import psycopg2

def setup_schema(connection_string):
    print("Loading data")
    conn = psycopg2.connect(connection_string)
    print ("Connection established")
    cursor = conn.cursor()
    print("Cursor created")
    with open('schema.sql', 'r') as schema:
        setup_queries = schema.read()
        cursor.execute(setup_queries)
        conn.commit()

def load_historical_stock_data(connection_string, stock_data_filename):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    with open(stock_data_filename, 'r') as stock_price_file:
        skip_first_line = True
        for line in stock_price_file:
            if skip_first_line:
                skip_first_line = False
                continue
            line_elements = line.split(',')
            elements_length = len(line_elements)
            # remove trailing \n on last element
            line_elements[elements_length - 1] = line_elements[elements_length - 1].strip('\n')
            cursor.execute("INSERT INTO historical_stock_prices VALUES(%(ticker)s, %(open)s, %(close)s, %(adj_close)s, %(low)s, "\
            "%(high)s, %(volume)s, %(trade_date)s);", {'ticker': line_elements[0], 'open': line_elements[1], 'close': line_elements[2], \
            'adj_close': line_elements[3], 'low': line_elements[4], 'high': line_elements[5], \
            'volume': line_elements[6], 'trade_date': line_elements[7]})
        conn.commit()
            
def load_company_information(connection_string, company_info_filename):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    with open(company_info_filename, 'r') as company_info_file:
        skip_first_line = True
        for line in company_info_file:
            if skip_first_line:
                skip_first_line = False
                continue
            line_elements = line.split(',')
            elements_length = len(line_elements)
            # remove trailing \n on last element
            line_elements[elements_length - 1] = line_elements[elements_length - 1].strip('\n')
            cursor.execute("INSERT INTO company_information VALUES(%(ticker)s, %(exchange)s, %(name)s, %(sector)s, %(industry)s);", \
            {'ticker': line_elements[0], 'exchange': line_elements[1], 'name': line_elements[2], \
            'sector': line_elements[3], 'industry': line_elements[4] })
        conn.commit()