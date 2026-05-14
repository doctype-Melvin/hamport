import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import time
from rapidfuzz import process, utils

load_dotenv()

# Get all unique locations and save them to CSV

engine = create_engine(os.getenv("SUPA_URL"))

query = """
    SELECT DISTINCT
        airport_location
    FROM analytics.fct_flights
"""
print("Connecting to database")
df_unique = pd.read_sql(query, engine)

df_unique = df_unique.sort_values('airport_location')

df_unique.to_csv('./dbt-hamport/seeds/airport_mapping.csv', index=False)
print("Query successful. Continuing in 3 seconds...")
time.sleep(3)

print("Cleaning airport location names")
df = pd.read_csv('./dbt-hamport/seeds/airport_mapping.csv')
unique_names = sorted(df['airport_location'].tolist())

mapping_data = []
processed = set()

for name in unique_names:
    if name in processed:
        continue
    
    # 1. Find similar names in the rest of the list
    # We look for names with > 85% similarity
    matches = process.extract(
        name, 
        unique_names, 
        score_cutoff=80, 
        processor=utils.default_process
    )
    
    # 2. The first match is usually the name itself. 
    # If there are other matches, they are potential duplicates.
    standardized_name = name # We pick the first one as the 'truth'
    
    for match_name, score, index in matches:
        mapping_data.append({
            'airport_location': match_name,
            'standardized_location': standardized_name
        })
        processed.add(match_name)

# 4. Save the automated suggestion
pd.DataFrame(mapping_data).to_csv('./dbt-hamport/seeds/airport_mapping.csv', index=False)

print("Finished!")