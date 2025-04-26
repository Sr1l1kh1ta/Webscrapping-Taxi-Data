from fastapi import APIRouter, HTTPException,Query
from typing import List, Dict, Type, Any
from app.schema import get_schema_from_mongodb
from app.db import fetch_all_data_feb, fetch_data_by_vendor_id,fetch_all_data_mar,fetch_trips_with_duration_feb,fetch_trips_with_duration_mar,collection,mar_collection,convert_to_dict
from pydantic import BaseModel
from pymongo import ReturnDocument

router = APIRouter()

@router.get("/data/feb", response_model=List[Dict[str, Any]])
async def get_data(limit: int = Query(20, le=100)):
    data = await fetch_all_data_feb(limit=limit)
    return data
@router.get("/data/mar", response_model=List[Dict[str, Any]])
async def get_data(limit: int = Query(20, le=100)):
    data = await fetch_all_data_mar(limit=limit)
    return data

@router.get("/data/vendor/{VendorID}", response_model=List[Dict[str, Any]])
async def get_data_by_vendor_id(VendorID: int):
    data_items = await fetch_data_by_vendor_id(VendorID)
    if not data_items:
        raise HTTPException(status_code=404, detail="No items found for the given vendor_id")
    return data_items

@router.get("/schema")
async def get_schema():
    model = await get_schema_from_mongodb()
    if model is None:
        raise HTTPException(status_code=404, detail="No data found to infer schema")
    return model.schema()

@router.get("/feb/duration", response_model=List[Dict[str, Any]])
async def get_trips_duration(limit: int = Query(20, le=100)):
    try:
        trips = await fetch_trips_with_duration_feb(limit)  # No need for await
        if not trips:
            raise HTTPException(status_code=404, detail="No trips found")
        return trips
    except Exception as e:
        print(f"Exception: {e}")  # Debugging line
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

@router.get("/mar/duration", response_model=List[Dict[str, Any]])
async def get_trips_duration(limit: int = Query(20, le=100)):
    try:
        trips = await fetch_trips_with_duration_mar(limit)  # No need for await
        if not trips:
            raise HTTPException(status_code=404, detail="No trips found")
        return trips
    except Exception as e:
        print(f"Exception: {e}")  # Debugging line
        raise HTTPException(status_code=500, detail="Internal Server Error")
  
from bson import ObjectId
@router.delete("/delete/trip/{trip_id}", status_code=204)
async def delete_trip(trip_id: str):

    try:
        # Convert string trip_id to ObjectId if needed
        if len(trip_id) == 24:  # Check if trip_id is a valid ObjectId length
            trip_id = ObjectId(trip_id)

        result = await collection.delete_one({"_id": trip_id})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Trip not found")
        return {"detail": "Trip deleted successfully"}
    except Exception as e:
        print(f"Error: {e}")  # Print the exception for debugging
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
class FieldUpdate(BaseModel):
    field: str
    value: Any

@router.put("/put/trip/{trip_id}", response_model=Dict[str, Any])
async def update_trip(trip_id: str, field_update: FieldUpdate):

    try:
        # Convert the trip_id to ObjectId if it's in string format
        if len(trip_id) == 24:
            trip_id = ObjectId(trip_id)

        # Prepare the update data
        update_data = {field_update.field: field_update.value}

        # Perform the update operation
        result = await collection.find_one_and_update(
            {"_id": trip_id},
            {"$set": update_data},
            return_document=ReturnDocument.AFTER
        )

        if not result:
            raise HTTPException(status_code=404, detail="Trip not found")

        # Convert ObjectId to string for response
        result["_id"] = str(result["_id"])
        
        return result
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

@router.post("/trip/")
async def create_trip(trip_data: dict):
    # Retrieve the dynamic model based on the current schema
    DynamicModel = await get_schema_from_mongodb()
    
    if DynamicModel is None:
        raise HTTPException(status_code=500, detail="Failed to retrieve schema from MongoDB")

    # Check if '_id' is present in the provided data
    if '_id' in trip_data:
        # Ensure _id is in ObjectId format for MongoDB
        try:
            trip_data['_id'] = ObjectId(trip_data['_id'])
        except Exception as e:
            raise HTTPException(status_code=400, detail="Invalid _id format")

    try:
        # Validate and create the trip instance
        trip_instance = DynamicModel(**trip_data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid data: {e}")

    # Insert the new document into MongoDB
    await collection.insert_one(trip_instance.dict())

    return {"inserted_id": str(trip_instance.get('_id'))}
