import psycopg2

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

# TODO add your code here (or in other files, at your discretion) to load the data


def main():
    # TODO invoke your code to load the data into the database
    print("Loading data")
    conn = psycopg2.connect(connection_string)
    print ("Connection established")
    cursor = conn.cursor()
    print("Cursor created")
    with open('schema.sql', 'r') as schema:
    	setup_queries = schema.read()
    	cursor.execute(setup_queries)
    	conn.commit()

if __name__ == "__main__":
    main()
