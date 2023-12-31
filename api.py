from dotenv import load_dotenv
import os
import requests
import pandas as pd
import time

load_dotenv()

## PARAMS
## start=Skip N number of rows. Useful for pagination. Default is 0
## limit=Limit number of rows to be returned. Default is 20. Maximum allowed is 200
## sort=Sort by one of the field of the data. Format is FIELDNAME ORDER(asc/desc) . Example myFieldName asc
## filter=Filter by one of the field of the data. Format is FIELDNAME:VALUE . Example myFieldName:myValue Comma separated for multiple filters

headers = {
    'Authorization': f'Bearer {os.getenv("access_token")}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

def get_headers():
    return headers

# Location of Mobility as a Service (MaaS) Cyberjay with ride-pooling transportation (kumpool) and shared escooter service (TRYKE)
# res = requests.get('https://api.oip.tmrnd.com.my/t/datahub.oip.tmrnd.com.my/MaaS-Cyberjaya/1.0.0/1680015862264/kumpool/data?start=1&limit=100&sort=stopname%20asc&filter=stopname%3ACyberjaya*', headers=headers)


# Kumpool ride-pooling transportation data at Cyberjaya from Feb to July 2023
## Same params as above, start, stop, limit, filter.
# res = requests.get("https://api.oip.tmrnd.com.my/t/cyberview.com.my/kumpool/1.0.0/2074418464416/data", headers=headers)


# TRYKE Trips in Cyberview for 6 months
## Same params as above, start, stop, limit, filter.
# res = requests.get("https://api.oip.tmrnd.com.my/t/cyberview.com.my/tryke/1.0.0/2424560862581/properties", headers=headers)
# res = requests.get("https://api.oip.tmrnd.com.my/t/cyberview.com.my/tryke/1.0.0/2424560862581/data", headers=headers)


# Cyberview Bus Stop location in long and lat as well as the name of the road
# res = requests.get("https://api.oip.tmrnd.com.my/t/datahub.oip.tmrnd.com.my/Cyberview-BusStop/1.0.0/2399804818698/data", headers=headers)


# Car counting for traffic system over selected location in Cyberjaya
# res = requests.get("https://api.oip.tmrnd.com.my/t/cyberview.com.my/traffic/1.0.0/1737658701995/data", headers=headers)


def write_data(filename: str, response):
    with open(filename, "w") as f:
        f.write(response.text)
        



def extract_traffic():
    def extract():
        limit = 200
        returned = 10000
        counter = 0
        
        df = pd.DataFrame()
        
        while returned >= 200:
            request = f"https://api.oip.tmrnd.com.my/t/cyberview.com.my/traffic/1.0.0/1737658701995/data?start={counter * 200}&limit={limit}"
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
                    "datetime": [ride["time"]],
                    "location": [ride["location"]],
                    "num_cars": [ride["car"]],
                })
                df = pd.concat([df, new_df])
            df.to_csv("traffic.csv", mode="a", header=False, index=False)
            
    extract()


if __name__ == "__main__":
    # extract_cyberview_bus_stops()
    # extract_kumpool()
    # extract_tryke()
    # extract_traffic()
    pass