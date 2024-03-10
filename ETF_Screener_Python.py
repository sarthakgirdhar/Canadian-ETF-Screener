""" Scraping Wikipedia """

# import libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd

# wikipedia url to scrape data from
wiki_url = "https://en.wikipedia.org/wiki/List_of_Canadian_exchange-traded_funds"

wiki_response = requests.get(wiki_url)

wiki_response.status_code

# parse data from the html into a beautifulsoup object
soup = BeautifulSoup(wiki_response.text, 'html.parser')
wiki_etf_table = soup.find('table',{'class':"wikitable"})

print(wiki_etf_table)

# convert wikipedia table into a pandas dataframe
wiki_etf_df = pd.read_html(str(wiki_etf_table))

# view the contents of the dataframe. The dataframe is currently a list.
print(wiki_etf_df)

# convert list to dataframe
wiki_etf_df = pd.DataFrame(wiki_etf_df[0])

wiki_etf_df.shape

wiki_etf_df.head()


""" Transforming wiki_etf_df """

# drop redundant columns from the dataframe
wiki_etf_df.drop(wiki_etf_df.loc[:, 'Inverse':'Active Managed'].columns, inplace=True, axis=1)

# remove first 5 characters/extract string from 6th character onwards
wiki_etf_df['Symbol'] = wiki_etf_df['Symbol'].str[5:]

wiki_etf_df.head()

# remove last character
wiki_etf_df['MER'] = wiki_etf_df['MER'].str[:-1]

# rename column
wiki_etf_df.rename({'MER': 'MER (%)'}, axis=1, inplace=True)

# check for unique values
wiki_etf_df['MER (%)'].unique()

wiki_etf_df['MER (%)'].replace(
    to_replace=['-'],
    value='0.00',
    inplace=True
)

wiki_etf_df['MER (%)'].replace(
    to_replace=['0.03% (after mgmt fee rebate'],
    value='0.03',
    inplace=True
)

wiki_etf_df['MER (%)'].unique()

wiki_etf_df.head()


""" Data Validation for wiki_etf_df """

# check if there are any null values in all the columns of the dataframe
wiki_etf_df.isnull().any()

wiki_etf_df[wiki_etf_df['Issuer'].isnull()]

wiki_etf_df['Issuer'].unique()

wiki_etf_df.loc[wiki_etf_df.index[121], 'Issuer'] = "Horizons"

wiki_etf_df.loc[121,:]

wiki_etf_df[wiki_etf_df['Total Assets (MM)'].isnull()]

wiki_etf_df['Total Assets (MM)'] = wiki_etf_df['Total Assets (MM)'].fillna(0)

wiki_etf_df['Asset Class'].unique()

wiki_etf_df.isnull().any()


""" Scraping ca.investing.com """

investing_url = "https://ca.investing.com/etfs/canada-etfs"

header = {
    'authority': 'www.google.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    # Add more headers as needed
}

investing_response = requests.get(investing_url, headers=header)

investing_response.status_code

print(investing_response.text)

# url to scrape data from
investing_url = "https://webcache.googleusercontent.com/search?q=cache:https://ca.investing.com/etfs/canada-etfs"

investing_response = requests.get(investing_url, headers=header)

investing_response.status_code

print(investing_response.text)

# parse data from the html into a beautifulsoup object
soup = BeautifulSoup(investing_response.text, 'html.parser')
investing_etf_table = soup.find('table',{'id':"etfs"})

print(investing_etf_table)

investing_etf_df = pd.read_html(str(investing_etf_table))

print(investing_etf_df)

# convert list to dataframe
investing_etf_df = pd.DataFrame(investing_etf_df[0])

investing_etf_df.shape

investing_etf_df.head()


""" Transforming investing_etf_df """

# delete redundant columns
investing_etf_df.drop(investing_etf_df.columns[[0, 1, 6, 7]], axis=1, inplace=True)

# rename column Last -> Price
investing_etf_df.rename({'Last': 'Price'}, axis=1, inplace=True)

investing_etf_df.head()

# remove last character of the string
investing_etf_df['Chg. %'] = investing_etf_df['Chg. %'].str[:-1]

investing_etf_df['Vol.'].unique()

investing_etf_df['Vol.'] = investing_etf_df['Vol.'].replace({'K': '*1e3', 'M': '*1e6'}, regex=True).map(pd.eval).astype(float)

investing_etf_df.head(55)


""" Data Validation for investing_etf_df """

# check if there are any null values in all the columns of the dataframe
investing_etf_df.isnull().any()

# check for any inconsistencies/errors in all the columns of the dataframe

investing_etf_df['Symbol'].unique()

investing_etf_df['Price'].unique()

investing_etf_df['Chg. %'].unique()

investing_etf_df['Vol.'].unique()


""" Loading the two pandas dataframes as two separate tables in Snowflake """

# import libraries
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# create a function to upload the 'wiki_etf_df' dataframe into a table in Snowflake
def wiki_to_snowflake(ACCOUNT, USER, PASSWORD, WAREHOUSE, DATABASE, SCHEMA):
    
    # connect to Snowflake
    conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )
    
    # create a cursor
    cur = conn.cursor()
    
    # create the warehouse
    cur.execute(f'CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 300')
    
    # use the warehouse
    cur.execute(f'USE WAREHOUSE {WAREHOUSE}')
    
    # create the database
    cur.execute(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
    
    # use the database
    cur.execute(f'USE DATABASE {DATABASE}')
    
    # create the schema
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')
    
    # use the schema
    cur.execute(f'USE SCHEMA {SCHEMA}')
    
    # create the table
    cur.execute("""
    CREATE OR REPLACE TABLE wiki_etf (
        "Symbol" STRING,
        "Name" STRING,
        "Issuer" STRING,
        "Asset Class" STRING,
        "Inception Date" DATE,
        "Total Assets (MM)" FLOAT,
        "MER (%)" FLOAT
        ) 
    """)
    
    # load the data from 'wiki_etf_df' dataframe into 'wiki_etf' Snowflake table
    
    cur.execute('TRUNCATE TABLE wiki_etf')  # clear existing data if needed
    
    write_pandas(conn, wiki_etf_df, 'WIKI_ETF')
    
    
    # close the cursor and Snowflake connection
    cur.close()
    conn.close()
    
# call the function
wiki_to_snowflake('Y*****A-Y*****9', 'sarthakgirdhar', 'XXXXXXXX', 'COMPUTE_WH','CanadianETFScreener', 'Wikipedia')


# create a function to upload the 'investing_etf_df' dataframe into a table in Snowflake
def investing_to_snowflake(ACCOUNT, USER, PASSWORD, WAREHOUSE, DATABASE, SCHEMA):
    
    # connect to Snowflake
    conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )
    
    # create a cursor
    cur = conn.cursor()
    
    # create the warehouse
    cur.execute(f'CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 300')
    
    # use the warehouse
    cur.execute(f'USE WAREHOUSE {WAREHOUSE}')
    
    # create the database
    cur.execute(f'CREATE DATABASE IF NOT EXISTS {DATABASE}')
    
    # use the database
    cur.execute(f'USE DATABASE {DATABASE}')
    
    # create the schema
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA}')
    
    # use the schema
    cur.execute(f'USE SCHEMA {SCHEMA}')
    
    # create the table
    cur.execute("""
    CREATE OR REPLACE TABLE investing_etf (
        "Symbol" STRING,
        "Price" FLOAT,
        "Chg. %" FLOAT,
        "Vol." FLOAT
        ) 
    """)
    
    # load the data from 'investing_etf_df' dataframe into 'investing_etf' Snowflake table
    
    cur.execute('TRUNCATE TABLE investing_etf')  # clear existing data if needed
    
    write_pandas(conn, investing_etf_df, 'INVESTING_ETF')
    
    
    # close the cursor and Snowflake connection
    cur.close()
    conn.close()
    
# call the function
investing_to_snowflake('Y*****A-Y*****9', 'sarthakgirdhar', 'XXXXXXXX', 'COMPUTE_WH','CanadianETFScreener', 'Investing')
