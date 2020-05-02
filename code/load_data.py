import psycopg2
import setup

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

# TODO add your code here (or in other files, at your discretion) to load the data


def main():
<<<<<<< HEAD
    # TODO invoke your code to load the data into the database
    setup.setup_schema(connection_string)
    print ("Schema initialized")
    print("Loading company information data")
    setup.load_company_information(connection_string, "datasets/historical_stocks.csv")
    print ("Company information data successfully loaded. Loading historical stock price data")
    setup.load_historical_stock_data(connection_string, "datasets/historical_stock_prices.csv")
    print ("Historical stock price data successfully loaded")

if __name__ == "__main__":
    main()
