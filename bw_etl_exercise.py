import requests
from bs4 import BeautifulSoup
from datetime import datetime
import csv

url_scrape = 'https://naccrrapps.naccrra.org/navy/directory/programs.php?program=omcc&state=CA'
url_json = 'https://bw-interviews.herokuapp.com/data/providers'
timestamp = datetime.now() # Timestamp used for metadata of ETL pipeline
file_csv = './x_ca_omcc_providers.csv'
output_csv = './output.csv'


def divide_chunks(l, n):
    """
    Takes a list of unchunked items and chunks them into a list of lists 
    to match the ideal format of your use case.

    Parameters
    ----------
    l: list
        Non-chunked list
    n: int
        Number of items in a chunk

    Yields
    -------
    list
        Chunked entries from HTML scrape
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def parse_html(url):
    """
    Scrapes HTML page to convert table to dictionary format for load.

    Parameters
    ----------
    url: string
        HTML source

    Returns
    -------
    dict
        Parsed dictionary of items scraped from URL
    """
    r = requests.get(url, verify=False)

    # Using bs4 to parse the HTML
    soup = BeautifulSoup(r.content, 'html.parser')
    # Grab the table. This was tough because the table is not tagged or a part of a class.
    table = soup.table

    # Converted this entire thing to a string because the table rows are not delimited with 
    # <tr> as they should be. This added some complexity to parsing this table.  I would 
    # not have had to chunk the table back up as shown below if the table was properly formatted.
    table_rows = str(table.find_all('td'))
    table_rows = table_rows.replace('<td>', '').replace('</td>', '').replace("[", '').replace("]", '')
    table_rows = table_rows.split(",")

    # Chunking up the table into rows of 8 columns (which was observed at the source)
    n = 8
    table_rows = list(divide_chunks(table_rows, n))
    
    seq = 129038 # Simple sequence to generate an identifier (as found in other data sources)
    
    # Now we convert the list to a dictionary and trim the entries
    output_list = []
    for i in table_rows:
        output = {}
        output["id"] = f"html_{seq}"
        output["provider_name"] = i[0].strip()
        output["provider_type"] = i[1].strip()
        output["address"] = i[2].strip()
        output["city"] = i[3].strip()
        output["state"] = i[4].strip()
        output["zip"] = i[5].strip()
        output["phone"] = i[6].strip()
        output["email"] = i[7].strip()
        output_list.append(output)
        seq += 1

    return output_list


def parse_json(url):
    """
    Loads JSON from URL location to combine in load.

    Parameters
    ----------
    url: string
        HTML source

    Returns
    -------
    dict
        Parsed dictionary of items gathered from API endpoint
    """

    # This one was actually quite simple as it was already in a key:value format
    # which I had decided already was the path I would take to ETL this data.
    r = requests.get(url, verify=False)
    result = r.json()
    output_list = result["providers"]

    return output_list


def parse_csv(filepath):
    """
    Loads CSV from file location to combine in load.

    Parameters
    ----------
    filepath: string
        File source of CSV file

    Returns
    -------
    dict
        Parsed dictionary of items gathered from CSV file
    """
    seq = 129038 # Simple sequence to generate an identifier (as found in other data sources)

    # Similar approach to the HTML parsing but without the complexity of parsing a 
    # non-structured format.
    output_list = []
    with open(filepath, "r") as f:
        reader = csv.reader(f, delimiter=',')
        data = list(reader)
        for i in data:
            output = {}
            output["id"] = f"csv_{seq}"
            output["provider_name"] = i[0].strip()
            output["provider_type"] = i[1].strip()
            output["address"] = i[2].strip()
            output["city"] = i[3].strip()
            output["state"] = i[4].strip()
            output["zip"] = i[5].strip()
            phone = i[6].strip()
            # Reformatting phone to match others. In retrospect, 
            # I should probably have converted them all 
            # to 10 digit int format.
            output["phone"] = f"({phone[0:3]}) {phone[3:6]}-{phone[6:10]}"
            output_list.append(output)
            seq += 1
    
    return output_list


def write_to_csv(input, output_file, source, ts, append=True):
    """
    Writes output(s) to CSV in a common format.

    Parameters
    ----------
    input: dict
        Dictionary representing one source of data
    output_file: string
        Location of output in CSV format
    source: string
        Source of the data being exported
    ts: timestamp
        Timestamp of when data was extracted
    append: bool
        Boolean for file interaction as append or not

    """
    # This is simply to handle overwriting the file each time this is run.
    # In a non-exercise situation, we are probably making more elegant choices 
    # about storage, archiving, and purging of data. This is just a little hack
    # so that you can rerun this script until the cows come home.
    if append:
        action = "a"
    else:
        action = "w"

    # Simply decompose the dictionary into rows for the CSV
    for entry in input:
        with open(output_file, action) as fo:
            file_writer = csv.writer(fo, delimiter=',', quotechar='"')
            file_writer.writerow([
                entry.get('id', None),
                entry.get('provider_name'), # Not optional
                entry.get('provider_type', None),
                entry.get('address', None),
                entry.get('city', None),
                entry.get('state', None),
                entry.get('zip', None),
                entry.get('phone'), # Not optional
                entry.get('email', None),
                entry.get('owner_name', None),
                source,
                ts
                ])	

def testing():
    '''
    Didn't get time to build this out but as illustration, there are a few checks I would do right off the bat:
    - Verify the source and destination counts match for each movement
    - Verify uniqueness of the PK I used and created
    - Verify uniqueness of phone number which I intend to use to clean and deduplicate this further
    - Add data type verification into each step (e.g., is the phone number an INT? does the email follow an email format?) 
        By doing this prior to loading, we could spit out invalid lines prior to the load and process them separately.
    '''


if __name__ == '__main__':
    header = [{"id":"id", "provider_name":"provider_name", "provider_type":"provider_type", "address":"address", "city":"city", "zip":"zip", "phone":"phone", "email":"email", "owner_name":"owner_name"}]
    write_to_csv(header, output_csv, "source", "timestamp", append=False)
    write_to_csv(parse_json(url_json), output_csv, 'internal_api', timestamp)
    write_to_csv(parse_html(url_scrape), output_csv, 'external_web', timestamp)
    write_to_csv(parse_csv(file_csv), output_csv, 'external_csv', timestamp)
