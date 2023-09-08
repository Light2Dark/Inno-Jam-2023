import requests
import pandas as pd
import time
from api import get_headers
import datetime

def extract():
    headers = get_headers()
    
    limit = 200
    returned = 10000
    counter = 0
    
    df = pd.DataFrame()
    
    while returned >= 200:
        request = f"https://api.oip.tmrnd.com.my/t/cyberview.com.my/tryke/1.0.0/2424560862581/data?start={counter * 200}&limit={limit}"
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
                "IMEI": [ride["IMEI"]],
                "user_location_on_booking_latitude": [ride["userLocationAtBookingLat"]],
                "user_location_on_booking_longitude": [ride["userLocationAtBookingLong"]],
                "user_location_on_dropoff_latitude": [ride["userLocationAtDropOffLat"]],
                "use_location_on_dropoff_longitude": [ride["userLocationAtDropOffLong"]],
                "pickup_date": [ride["pickUpDate"]],
                "pickup_time": [ride["pickUpTime"]],
                "total_duration": [ride["totalDuration"]],
                "dropoff_date": [ride["dropOffDate"]],
                "dropoff_time": [ride["dropOffTime"]],
            })
            df = pd.concat([df, new_df])
        df.to_csv("tryke.csv", mode="a", header=False, index=False)
        

def transform():
    df = pd.read_csv("csv_files/raw/tryke.csv")
    
    df['dropoff_datetime'] = df['dropoff_date'] + "," + df['dropoff_time']
    df['pickup_datetime'] = df['pickup_date'] + "," + df['pickup_time']
    date_format = "%d/%m/%Y,%I:%M:%S %p"
    
    def get_date(date_str: str):
        date = date_str.split(",")[0]
        time = date_str.split(",")[-1]
        hour = time.split(":")[0]
        if hour == "0":
            date_str = f"{date},12:{time.split(':')[1]}:{time.split(':')[2]}"
        return datetime.datetime.strptime(date_str,  date_format)
    
    # # replace 0: hour with 12: to indicate midnight
    df['dropoff_datetime'] = df['dropoff_datetime'].apply(get_date)
    df['pickup_datetime'] = df['pickup_datetime'].apply(get_date)
    
    df['user_on_booking_lat_long'] = df['user_location_on_booking_latitude'].astype(str) + "," + df['user_location_on_booking_longitude'].astype(str)
    df['user_on_dropoff_lat_long'] = df['user_location_on_dropoff_latitude'].astype(str) + "," + df['user_location_on_dropoff_longitude'].astype(str)
    df.drop(['user_location_on_booking_latitude', 'user_location_on_booking_longitude', 'user_location_on_dropoff_latitude', 'user_location_on_dropoff_longitude', 'dropoff_date', 'dropoff_time', 'pickup_date', 'pickup_time'], axis=1, inplace=True)
    
    df.to_csv("csv_files/transformed/tryke.csv", index=False)
    
if __name__ == "__main__":
    transform()
    
    date = "25/03/2023,0:07:51 am"
    date2 = "24/01/2023,0:36:30 pm"
    date3 = "24/01/2023,2:30:30 pm"
    date4 = "29/01/2023,0:41:06 am"
    # print(datetime.datetime.strptime(date3, "%d/%m/%Y,%I:%M:%S %p"))