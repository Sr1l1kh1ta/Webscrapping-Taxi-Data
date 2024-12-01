from motor.motor_asyncio import AsyncIOMotorClient
from typing import List,Dict,Any,Optional
from fastapi import Query
from datetime import datetime

MONGO_DETAILS = "mongodb://localhost:27017"


client = AsyncIOMotorClient(MONGO_DETAILS)
database = client.TripRecords  # Replace with your database name
collection = database.February  # Replace with your collection name
mar_collection = database.March

def convert_to_dict(document):
    document["_id"] = str(document["_id"])
    return document

async def fetch_all_data_feb(limit: int = Query(20, le=100)) -> List[Dict[str, Any]]:
    cursor = collection.find({}).limit(limit)
    data = [convert_to_dict(doc) async for doc in cursor]
    return data

async def fetch_all_data_mar(limit: int = Query(20, le=100)) -> List[Dict[str, Any]]:
    cursor = mar_collection.find({}).limit(limit)
    data = [convert_to_dict(doc) async for doc in cursor]
    return data

async def fetch_data_by_vendor_id(VendorID: int) -> List[Dict[str, Any]]:
    cursor = collection.find({"VendorID": VendorID}).limit(20)
    data = [convert_to_dict(doc) async for doc in cursor]
    return data

def parse_datetime(datetime_str: str) -> Optional[datetime]:
    try:
        datetime_str = datetime_str.strip()
        return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        # Print detailed error message if parsing fails
        print(f"ValueError: Could not parse datetime string '{datetime_str}' - Error: {e}")
        return None
async def fetch_trips_with_duration_feb(limit: int = 20) -> List[Dict[str, Any]]:
    cursor = collection.find({}).limit(limit)
    trips = []
    async for doc in cursor:
        doc_dict = convert_to_dict(doc)
        pickup_time = doc_dict.get('tpep_pickup_datetime', None)
        dropoff_time = doc_dict.get('tpep_dropoff_datetime', None)

        # Debugging: Print the datetime values
        print(f"Pickup time: {pickup_time}")
        print(f"Dropoff time: {dropoff_time}")

        if isinstance(pickup_time, datetime) and isinstance(dropoff_time, datetime):
            duration_ms = int((dropoff_time - pickup_time).total_seconds() * 1000)
            trip_duration = duration_ms
        else:
            trip_duration = None

        # Filter and include only the required fields
        filtered_doc = {
            '_id': doc_dict.get('_id'),
            'VendorID': doc_dict.get('VendorID'),
            'total_amount': doc_dict.get('total_amount'),
            'Trip_duration_milliseconds': trip_duration
        }

        trips.append(filtered_doc)
    return trips


async def fetch_trips_with_duration_mar(limit: int = 20) -> List[Dict[str, Any]]:
    cursor = mar_collection.find({}).limit(limit)
    trips = []
    async for doc in cursor:
        doc_dict = convert_to_dict(doc)
        pickup_time = doc_dict.get('tpep_pickup_datetime', None)
        dropoff_time = doc_dict.get('tpep_dropoff_datetime', None)

        if isinstance(pickup_time, datetime) and isinstance(dropoff_time, datetime):
            duration_ms = int((dropoff_time - pickup_time).total_seconds() * 1000)
            trip_duration = duration_ms
        else:
            trip_duration = None

        # Filter and include only the required fields
        filtered_doc = {
            '_id': doc_dict.get('_id'),
            'VendorID': doc_dict.get('VendorID'),
            'total_amount': doc_dict.get('total_amount'),
            'Trip_duration_milliseconds': trip_duration
        }

        trips.append(filtered_doc)
    return trips
