import psycopg2
import setup

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

# TODO add your code here (or in other files, at your discretion) to load the data


def main():
	# TODO invoke your code to load the data into the database
	setup.setup_schema(connection_string)
	print ("Schema initialized")
	# setup.load_attack_data(connection_string, 'globalterrorismdb_0718dist.csv')
    setup.load_stock_data(connection_string, "datasets/historical_stock_prices.csv")

if __name__ == "__main__":
    main()
