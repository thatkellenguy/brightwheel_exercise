# Bright Wheel Exercise

Thanks for taking the time to look at my project!

What I got done:
- Parsed, transformed (where necessary), and combined all three sources into one CSV output
- Scraped the HTML source despite inconsistencies in the HTML format (improper table rows)
- Converted phone numbers to common format
- Aligned common data elements in one output
- Created a unique ID for 2 sources without one (CSV, HTML)
- Docstrings and commented code

What I ran out of time for:
- Loading this into a durable data store (like a database)
- Deduplication across the 3 data sets (would have used phone number as the identifier to deduplicate on)
    - Deduplication could (or perhaps should) be done in the database using SQL during the data modeling process
- Incremental loading of this data which could have also occurred during deduplication in the data modeling process
    - This is currently designed as a complete batch load
- Was unable to parse the HTML for page total and create a process to loop through all pages to load, only able to get one example page
    - For what it's worth, I would have parse the "Page N of NN" cell on the page and done a simple loop editing the URL source each time
- Gather SSL certs to avoid warnings

What I learned:
- Very little prior experience in HTML scraping so what you see is barely scratching the surface of what I'll be able to learn
- Learned a new library (BeautifulSoup)

What I felt very confident in:
- JSON and CSV parsing and loading
- Creating flexible functions for parsing lists (e.g., n chunk list function, writing csv function)
- Combinine all inputs into a common format

What I didn't see that I thought I would:
- More "dirty data".  These inputs were actually pretty clean and most of the challenge was realigning the data and learning to scrape HTML.

## But, thatkellenguy, how do I run it?

Clone this repository.

`pipenv install`

`pipenv run python3 bw_etl_exercise.py`