import pandas as pd
from rapidfuzz import utils, process
import re

df = pd.read_csv('../messy_flights.csv')
input = df["airline"].unique().tolist()

def pre_clean_names(name):
    if not name: return ""

    name = name.upper()

    if 'CITY AIRLINES' in name:
        return 'Lufthansa'
    elif 'FREEBIRD' in name:
        return 'FREEBIRD'
    elif 'SCANDINAVIAN AIRLINES' in name:
        return 'SAS'
    
    name = re.sub(r'\(.*?\)', '', name)
    return name.strip()

def group_airlines(names, threshold=90):
    mapping = {}
    clean_names = {pre_clean_names(n): n for n in names}
    sorted_names = sorted(clean_names.keys(), key=len)

    for name in sorted_names:
        if name in mapping:
            continue

        matches = process.extract(
            name,
            sorted_names,
            processor=utils.default_process,
            score_cutoff=threshold
        )

        for match_name, score, index in matches:
            if len(name) <= 3 and len(match_name) > len(name) + 2:
                continue
            mapping[match_name] = name

    final_map = {raw: mapping[pre_clean_names(raw)] for raw in names}

    return final_map

auto_map = group_airlines(input)

validate = []
for original, grouped in auto_map.items():
    validate.append({
        'airline': original,
        'group': grouped
    })

validation_df = pd.DataFrame(validate)
print(validation_df.to_string(index=False))

# df['airline_group'] = df['airline'].map(auto_map)

validation_df.to_csv('../out.csv')
print("Seed file generated")