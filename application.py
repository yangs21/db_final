import psycopg2
import psycopg2.extras
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.dates as dates
from decimal import Decimal
from datetime import datetime

def count_country_attack(conn, country):
    cursor = conn.cursor()
    query1 = ''' 
    DROP TABLE IF EXISTS ret CASCADE; 
    SELECT country, date_trunc('year', date) as year, count(*) AS total_attacks, sum(number_killed) AS total_killed
    INTO TEMP TABLE ret
    FROM attack_data, attack_location, attacks
    WHERE attack_data.attackid = attack_location.attackid 
    AND attack_data.attackid = attacks.attackid 
    AND date > date '1970-01-01'
    AND date < date '1991-01-01'    
    GROUP BY country, date_trunc('year', date)
    ORDER BY country, date_trunc('year', date)
    '''
    cursor.execute(query1)
    query2 = '''
    SELECT country, year, total_attacks, total_killed
    FROM ret
    WHERE country = %s
    '''
    cursor.execute(query2, (country,))
    rows = cursor.fetchall();

    return rows

def plot_country_attack(total_attack, year, country):
    fig, ax1 = plt.subplots(figsize=(20, 6))
    years = mdates.YearLocator()
    months = mdates.MonthLocator()
    yearsFmt = mdates.DateFormatter('%Y')
    ax1.xaxis.set_major_locator(years)
    ax1.xaxis.set_major_formatter(yearsFmt)
    ax1.xaxis.set_minor_locator(months)

    for i in range(len(total_attack)):
        plt.plot(year[i], total_attack[i], label=country[i])
        plt.legend()
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Total Attacks')
        ax1.set(ylim=(0, 800))
    plt.title('10 Countries with the Most Terrorist Attacks')
    plt.show()

def country_attack_summary(conn):
    cursor = conn.cursor()
    query3 = '''
    DROP TABLE IF EXISTS ret CASCADE;
    SELECT country, date_trunc('year', date) as year, count(*) AS total_attacks, sum(number_killed) AS total_killed
    INTO TEMP TABLE ret
    FROM attack_data, attack_location, attacks
    WHERE attack_data.attackid = attack_location.attackid 
    AND attack_data.attackid = attacks.attackid 
    AND date > date '1970-01-01'
    AND date < date '1991-01-01'    
    GROUP BY country, date_trunc('year', date)
    ORDER BY country, date_trunc('year', date)    
    '''
    cursor.execute(query3)

    query4 = '''
    SELECT country, sum(total_attacks) AS SUMofATTACK
    FROM ret
    GROUP BY country
    ORDER BY SUMofATTACK DESC
    LIMIT 10
    '''
    cursor.execute(query4)
    rows = cursor.fetchall();
    return rows

def count_US_attack(conn, year_start, year_end):
    cursor = conn.cursor()
    query5 = ''' 
    DROP TABLE IF EXISTS ret CASCADE;
    SELECT country, date_trunc('month', date) as year, count(*) AS total_attacks, sum(number_killed) AS total_killed
    INTO TEMP TABLE ret
    FROM attack_data, attack_location, attacks
    WHERE attack_data.attackid = attack_location.attackid
    AND country = 'United States'
    AND attack_data.attackid = attacks.attackid 
    AND date > %s
    AND date < %s   
    GROUP BY country, date_trunc('month', date)
    ORDER BY country, date_trunc('month', date)
    '''
    cursor.execute(query5, (year_start, year_end,))

    query6 = '''
    SELECT country, year, total_attacks, total_killed
    FROM ret
    '''
    cursor.execute(query6)
    rows = cursor.fetchall();
    return rows

def nasdaq_year(conn, year_start, year_end):
    cursor = conn.cursor()
    query7 = '''
    SELECT date_trunc('month',trade_date) as month, sum(adj_close * volume)/ sum(volume) AS weight_avg
    FROM company_information, historical_stock_prices
    WHERE exchange = 'NASDAQ' AND company_information.ticker = historical_stock_prices.ticker
    AND trade_date > %s AND trade_date < %s
    GROUP BY date_trunc('month',trade_date)
    ORDER BY date_trunc('month',trade_date);
    '''
    cursor.execute(query7, (year_start, year_end,))
    rows = cursor.fetchall();
    return rows

def plot_nasdaq_US_attack(conn, month, us_attack, nasdaq):
    fig, ax1 = plt.subplots(figsize=(16, 4))
    months = mdates.MonthLocator()
    monthsFmt = mdates.DateFormatter('%m')
    ax1.xaxis.set_major_locator(months)
    ax1.xaxis.set_major_formatter(monthsFmt)
    plt.plot(month, us_attack, label ='Numer of Attack in US in 1977', color='aquamarine')
    plt.legend()
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Total Attacks')
    ax1.set(ylim=(0, 25))

    months = mdates.MonthLocator()
    monthsFmt = mdates.DateFormatter('%m')
    ax2 = ax1.twinx()
    ax2.xaxis.set_major_locator(months)
    ax2.xaxis.set_major_formatter(monthsFmt)

    plt.plot(month, nasdaq, label ='NASDAQ Volume Weighted Average Price (VWAP) in 1977', color='paleturquoise')
    plt.legend()
    ax2.set_xlabel('Month')
    ax2.set_ylabel('NASDAQ VWAP')
    ax2.set(ylim=(0.2, 0.4))
    plt.title('Relation of Terrorist Attacks in US with NASDAQ Price \n(Color from Animal Crossing)')
    plt.show()

def target_attack_summary(conn):
    cursor = conn.cursor()
    query8 = '''
    DROP TABLE IF EXISTS ret CASCADE;
    SELECT target_type, date_trunc('year', date) as year, count(*) AS total_attacks, sum(number_killed) AS total_killed
    INTO TEMP TABLE ret
    FROM attack_data, attack_location, attacks
    WHERE attack_data.attackid = attack_location.attackid 
    AND attack_data.attackid = attacks.attackid 
    AND date > date '1970-01-01'
    AND date < date '1991-01-01'    
    GROUP BY country, date_trunc('year', date)
    ORDER BY country, date_trunc('year', date)    
    '''
    cursor.execute(query8)

    query4 = '''
    SELECT country, sum(total_attacks) AS SUMofATTACK
    FROM ret
    GROUP BY country
    ORDER BY SUMofATTACK DESC
    LIMIT 10
    '''
    cursor.execute(query4)
    rows = cursor.fetchall();
    return rows


def main():
    connection_string = "host='localhost' dbname='dbms_final_project' user='dbms_project_user' password='dbms_password'"
    conn = psycopg2.connect(connection_string)
    '''
    rows1 = count_country_attack(conn, "United States")
    year1 = [r[1] for r in rows1]
    attack_number1 = [r[2] for r in rows1]

    rows2 = count_country_attack(conn, "Afghanistan")
    year2 = [r[1] for r in rows2]
    attack_number2 = [r[2] for r in rows2]

    year = []
    attack_number = []
    country = []

    year.append(year1)
    year.append(year2)
    attack_number.append(attack_number1)
    attack_number.append(attack_number2)
    country.append("United States")
    country.append("Afghanistan")
    '''
    rows = country_attack_summary(conn)
    ten_countries = []
    for r in rows:
        ten_countries.append(r[0])

    year = []
    total_attack = []
    for r in ten_countries:
        temp = count_country_attack(conn, r)
        temp_year = [r[1] for r in temp]
        temp_attack_number = [r[2] for r in temp]
        year.append(temp_year)
        total_attack.append(temp_attack_number)

    #plot_country_attack(total_attack, year, ten_countries)

    year_start = datetime.strptime('1977', '%Y')
    year_end = datetime.strptime('1978', '%Y')
    nasdaq = [r[1] for r in nasdaq_year(conn,year_start, year_end)]
    month = [r[0] for r in nasdaq_year(conn, year_start, year_end)]
    us_attack = [r[2] for r in count_US_attack(conn, year_start, year_end)]
    plot_nasdaq_US_attack(conn, month, us_attack, nasdaq)

    print(country_attack_summary(conn))

if __name__ == "__main__":
    main()

