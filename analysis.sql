-- I ran out of time to put this in a database so
-- for the purposes of example we will assume it was loaded
-- into a table called 'childcare_providers'.

-- The schema of this table will follow what you see in the 
-- attached output.csv.

-- Count of all childcare providers.  
SELECT count(*) 
FROM childcare_providers;
-- I ran out of time to parse all of the HTML pages in the 
-- exercise so counts will be short by around 1100 providers.


-- Counts by zip code ordered by count descending to find
-- the zip code with the most providers. 
SELECT zip, count(*) as cnt
FROM childcare_providers
GROUP BY zip
ORDER BY count(*) desc;