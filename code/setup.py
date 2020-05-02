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

def load_stock_data(connection_string, stock_data_filename):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()
    with open(stock_data_filename, 'r') as stock_price_file:
        for line in stock_price_file:
            line_elements = line.split(',')
            print(line_elements)