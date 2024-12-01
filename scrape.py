import requests
import pandas as pd
from bs4 import BeautifulSoup
from pymongo import MongoClient
import io

# Define the URL of the NYC TLC trip record data page
url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

# Send an HTTP request to the URL
response = requests.get(url)

# Set up MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['TripRecords']  # Your database name

if response.status_code == 200:
    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all links on the page
    links = soup.find_all('a')
    
    # Extract the href attributes of the links that point to Parquet files for February and March 2019
    parquet_links = [
        link.get('href') for link in links
        if 'parquet' in link.get('href', '') and ('2019-02' in link.get('href') or '2019-03' in link.get('href'))
    ]
    
    # Process each Parquet link
    for parquet_link in parquet_links:
        if not parquet_link.startswith('http'):
            parquet_link = "https://www.nyc.gov" + parquet_link
        
        # Download the Parquet file
        parquet_response = requests.get(parquet_link)
        
        if parquet_response.status_code == 200:
            # Read the Parquet file directly into a DataFrame with specified dtype
            try:
                df = pd.read_parquet(io.BytesIO(parquet_response.content), 
                                     dtype_backend='pyarrow')  # Use 'pyarrow' to avoid dtype issues

                # Convert DataFrame to records and insert into MongoDB
                # Determine the collection based on the month
                if '2019-02' in parquet_link:
                    collection = db['February']  # Collection for February
                elif '2019-03' in parquet_link:
                    collection = db['March']  # Collection for March
                records = df.to_dict(orient='records')
                collection.insert_many(records)
                print(f"Uploaded data from: {parquet_link} to MongoDB")
            except Exception as e:
                print(f"Error processing {parquet_link}: {e}")
        else:
            print(f"Failed to download: {parquet_link}")
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")
