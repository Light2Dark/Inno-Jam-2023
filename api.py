from dotenv import load_dotenv
import os
import requests

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

# Location of Mobility as a Service (MaaS) Cyberjay with ride-pooling transportation (kumpool) and shared escooter service (TRYKE)
# res = requests.get('https://api.oip.tmrnd.com.my/t/datahub.oip.tmrnd.com.my/MaaS-Cyberjaya/1.0.0/1680015862264/kumpool/data?start=1&limit=100&sort=stopname%20asc&filter=stopname%3ACyberjaya*', headers=headers)


# Kumpool ride-pooling transportation data at Cyberjaya from Feb to July 2023
## Same params as above, start, stop, limit, filter.
# res = requests.get("https://api.oip.tmrnd.com.my/t/cyberview.com.my/kumpool/1.0.0/2074418464416/data", headers=headers)


# TRYKE Trips in Cyberview for 6 months
## Same params as above, start, stop, limit, filter.
res = requests.get("https://api.oip.tmrnd.com.my/t/cyberview.com.my/tryke/1.0.0/2424560862581/properties", headers=headers)


# Cyberview Bus Stop location in long and lat as well as the name of the road
res = requests.get("https://api.oip.tmrnd.com.my/t/datahub.oip.tmrnd.com.my/Cyberview-BusStop/1.0.0/2399804818698/data", headers=headers)


# Car counting for traffic system over selected location in Cyberjaya
res = requests.get("https://api.oip.tmrnd.com.my/t/cyberview.com.my/traffic/1.0.0/1737658701995/data", headers=headers)

print(res.json())