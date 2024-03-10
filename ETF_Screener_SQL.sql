SELECT * FROM CANADIANETFSCREENER.WIKIPEDIA.WIKI_ETF;

SELECT * FROM CANADIANETFSCREENER.INVESTING.INVESTING_ETF;

/* Join the two tables to create a new Snowflake table */
CREATE OR REPLACE TABLE CANADIANETFSCREENER.MERGED.ETF
AS 
(SELECT * FROM CANADIANETFSCREENER.WIKIPEDIA.WIKI_ETF
NATURAL JOIN CANADIANETFSCREENER.INVESTING.INVESTING_ETF
ORDER BY "Symbol");

SELECT * FROM CANADIANETFSCREENER.MERGED.ETF;


/* Which ETFs from 'Blackrock' have an MER of less than 0.25%? */
SELECT "Symbol", "Name", "MER (%)"
FROM CANADIANETFSCREENER.MERGED.ETF
WHERE "Issuer" = 'Blackrock'
AND "MER (%)" < 0.25
ORDER BY "MER (%)" DESC;

/* Which ETFs have the most Total Assets in the 'Fixed Income' Asset Class? */
SELECT "Symbol", "Name", "Total Assets (MM)"
FROM CANADIANETFSCREENER.MERGED.ETF
WHERE "Asset Class" = 'Fixed Income'
ORDER BY "Total Assets (MM)" DESC
LIMIT 15;

/* Which Issuer has the most Total Assets and how many ETFs offerings do they have? */
SELECT "Issuer", COUNT("Issuer") AS "Number of ETFs", SUM("Total Assets (MM)") AS "Assets (MM)"
FROM CANADIANETFSCREENER.MERGED.ETF
GROUP BY "Issuer"
ORDER BY 3 DESC;

/* Total Assets by Issuer and Asset Class */
SELECT "Issuer", "Asset Class", SUM("Total Assets (MM)") AS "Assets (MM)"
FROM CANADIANETFSCREENER.MERGED.ETF
GROUP BY GROUPING SETS (("Issuer", "Asset Class"))
ORDER BY 3 DESC;

/* Which was the third most traded ETF by Volume? */
WITH most_traded_ETF AS (
SELECT *,
   DENSE_RANK() OVER (ORDER BY "Vol." Desc) AS Rnk
FROM CANADIANETFSCREENER.MERGED.ETF
)
SELECT *
FROM most_traded_etf
WHERE Rnk=3;

/* Give me all the details of the oldest ETF from each Issuer */
WITH oldest_ETF AS (
SELECT *,
    DENSE_RANK() OVER (PARTITION BY "Issuer" ORDER BY "Inception Date" ASC) AS Rnk
    FROM CANADIANETFSCREENER.MERGED.ETF
)
SELECT * EXCLUDE "RNK"
FROM oldest_etf
WHERE Rnk = 1;
