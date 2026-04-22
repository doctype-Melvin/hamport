import requests
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB

HAM_API_URL = "https://api.hamburg-airport.de/v2/flights/arrivals"
HEADERS = {
    "Ocp-Apim-Subscription-Key": "HAM_API_KEY"
}