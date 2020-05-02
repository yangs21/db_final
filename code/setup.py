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