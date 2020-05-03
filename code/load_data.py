import psycopg2
import setup

connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"

# TODO add your code here (or in other files, at your discretion) to load the data


def main():
    # TODO invoke your code to load the data into the database
    setup.setup_schema(connection_string)
    print ("Schema initialized")
    print("Loading company information data...")
    setup.load_file(connection_string, [setup.company_information_query], "historical_stocks.csv")
    print ("Company information data successfully loaded.\nLoading historical stock price data...")
    setup.load_file(connection_string, [setup.historical_stock_prices_query], "historical_stock_prices.csv")
    print ("Historical stock price data successfully loaded.\nLoading global terrorism data...")
    setup.load_file(connection_string, [setup.attacks_query, setup.attack_location_query, setup.attack_data_query], "global_terrorism.csv", "cp1252")
    print ("Global terrorism data successfully loaded.")

if __name__ == "__main__":
    main()

