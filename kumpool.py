import requests
import pandas as pd
import time
from api import get_headers
import datetime

def extract(save_filepath: str):
    headers = get_headers()
    
    limit = 200
    returned = 10000
    counter = 0
    
    df = pd.DataFrame()
    
    while returned >= 200:
        request = f"https://api.oip.tmrnd.com.my/t/cyberview.com.my/kumpool/1.0.0/2074418464416/data?start={counter * 200}&limit={limit}"
        res = requests.get(request, headers=headers)
        print(request, res.status_code)
        
        retries = 0
        while res.status_code != 200:
            print("Retrying...")
            retries += 1
            res = requests.get(request, headers=headers)
            time.sleep(retries * 2)
            print(request, res.status_code)
        
        counter += 1

        data = res.json()
        rides = data["docs"]
        returned = data["returned"]
        df = pd.DataFrame()
        
        for ride in rides:
            new_df = pd.DataFrame({
                "datetime": [ride["Date"]],
                "from_stop": [ride["FromStop"]],
                "to_stop": [ride["ToStop"]],
                "riders": [ride["Riders"]],
                "total_km": [ride["TotalKM"]],
                "order_type": [ride["OrderType"]],
            })
            df = pd.concat([df, new_df])
        df.to_csv(save_filepath, mode="a", header=False, index=False)

def transform():
    df = pd.read_csv("csv_files/raw/kumpool.csv")
    
    date_format = "%a %b %d %H:%M:%S GMT %Y"
    df['datetime'] = pd.to_datetime(df['datetime'], format=date_format)
    
    def get_bus_station(name: str):
        last_nums = name.split("(")[-1]
        return last_nums.replace(')', '')
    
    df['station_from'] = df['from_stop'].apply(get_bus_station)
    df['station_to'] = df['to_stop'].apply(get_bus_station)
    
    df['riders'] = df['riders'].apply(lambda x: int(x))
    df.to_csv("csv_files/transformed/kumpool.csv", index=False)
    
if __name__ == "__main__":
    df = pd.read_csv("csv_files/transformed/kumpool.csv")
    print(df)