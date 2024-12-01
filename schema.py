from typing import Dict, Type
from pydantic import BaseModel, create_model
from app.db import collection

async def get_schema_from_mongodb() -> Type[BaseModel]:
    document = await collection.find_one()
    if not document:
        return None

    # Convert ObjectId to string for easier handling
    document["_id"] = str(document["_id"])
    fields = {}
    # Define fields based on document structure
    #fields = {key: (str, ...) for key, value in document.items()}
    for key, value in document.items():
        if not key.startswith('_') and key != "_id":
            # Determine type dynamically (adjust as necessary)
            if isinstance(value, str):
                fields[key] = (str, ...)
            elif isinstance(value, int):
                fields[key] = (int, ...)
            elif isinstance(value, float):
                fields[key] = (float, ...)
            elif isinstance(value, bool):
                fields[key] = (bool, ...)
            elif isinstance(value, list):
                fields[key] = (list, ...)
            elif isinstance(value, dict):
                fields[key] = (dict, ...)
            else:
                fields[key] = (str, ...)  # Fallback to string if type is unknown


    return create_model('DynamicModel', **fields)
