## Canadian ETF Screener

In this project, I build an ETL Pipeline by scraping/extracting data from [Wikipedia](https://en.wikipedia.org/wiki/List_of_Canadian_exchange-traded_funds) and [Investing.com](https://ca.investing.com/etfs/canada-etfs) using Python and Snowflake.

I start with scraping the two websites using Beautiful Soup and storing it as pandas dataframes. I then, do data transformations and perform some data validation, as per my requirement. I then, load the data into Snowflake as two separate tables. In Snowflake, the data is joined together to perform Exploratory Data Analysis (EDA) via SQL. 

### About the data

The table from Wikipedia contains information about the ETFs like, 'Ticker Symbol',	'Asset Management Company',	'Name of the ETF',	'Asset Class (Fixed Income, Equity, Commodity, etc.)',	'the Date the fund started', 	'Total Assets', and	'MER'.

Investing.com provides information like, 'Ticker Symbol', 'Last Price', 'Percent change in the last X hours', and 'Volume traded in the last X hours'. 

### Skills demonstrated

 - Understanding the data.
 - Writing code in Python to scrape/extract data from Wikipedia and Investing.com.
 - Checking for data quality and performing data transformations.
 - Creating a data warehouse in Snowflake so that it's easier for the downstream users to perform data analysis.
 - Data analysis - answering stakeholder’s questions.
 - SQL queries (Grouping Sets, Common Table Expressions, Window Functions, etc.)


   Read the detailed story in two parts - [part 1](https://ask-data.medium.com/data-engineering-building-an-etl-pipeline-for-canadian-etfs-229c5a94dab5) and [part 2](https://ask-data.medium.com/data-engineering-building-an-etl-pipeline-for-canadian-etfs-cc2567958ada).

