from fastapi import FastAPI
from app.routers import data

app = FastAPI()

app.include_router(data.router)
