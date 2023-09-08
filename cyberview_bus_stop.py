import requests
import pandas as pd
import time
from api import get_headers, write_data
import datetime

def extract_cyberview_bus_stops():
    headers = get_headers()
    
    res = requests.get("https://api.oip.tmrnd.com.my/t/datahub.oip.tmrnd.com.my/Cyberview-BusStop/1.0.0/2399804818698/data?limit=200", headers=headers)
    write_data("cyberview_bus_stops.json", res)
    
    df = pd.DataFrame()
    
    data = res.json()
    locations = data["docs"]
    
    for location in locations:
        new_df = pd.DataFrame({
            "street_name": [location["NamaJalan"]],
            "address": [location["Alamat"]],
            "point_of_interest": [location["POI"]],
            "latitude": [location["Latitude"]],
            "longitude": [location["Longitude"]],
            "additional_info": [location["Notes"]]
        })
        df = pd.concat([df, new_df])
        
    df.to_csv("cyberview_bus_stops.csv", index=False)
    
    
def transform():
    df = pd.read_csv("csv_files/raw/cyberview_bus_stops.csv")
    
    df['lat_long'] = df['latitude'].astype(str) + ',' + df['longitude'].astype(str)
    df = df.drop_duplicates(subset=['lat_long'])
    df.drop(columns=['latitude', 'longitude'], inplace=True)
    df['street_name'] = df['street_name'].str.title()
    df['point_of_interest'] = df['point_of_interest'].str.title()
    
    df.to_csv("csv_files/transformed/cyberview_bus_stops.csv", index=False)
    
    
if __name__ == "__main__":
    transform()