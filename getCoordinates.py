import requests
import pandas as pd
import json
import time
import logging
import argparse
from pyproj import Proj, Transformer
import random

#


# vpn_settings = initialize_VPN()

# %%


def get_random_user_agent():
    user_agents = [
        # List of various user agents
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 11; Pixel 4a) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Mobile Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:68.0) Gecko/20100101 Firefox/68.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
        "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.93 Mobile Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
    ]
    return random.choice(user_agents)


def convert_to_regular_coordinates(x, y, zone=32, ellps="WGS84"):
    # Define the UTM and geographic coordinate systems
    utm_proj = Proj(proj="utm", zone=zone, datum=ellps)
    latlong_proj = Proj(proj="latlong", datum=ellps)

    # Create a transformer object
    transformer = Transformer.from_proj(utm_proj, latlong_proj)

    # Transform the coordinates
    lon, lat = transformer.transform(x, y)

    return (lat, lon)

# %%


def get_vnb_from_plattform(lon, lat):
    return (lat, lon)


# %%
def get_query_selector():
    query = """
    fragment vnb_Region on vnb_Region {
        _id
        name
        logo {
            url
        }
        bbox
        layerUrl
        slug
        vnbs {
            _id
        }
    }

    fragment vnb_VNB on vnb_VNB {
        _id
        name
        logo {
            url
        }
        services {
            type {
                name
                type
            }
            activated
        }
        bbox
        layerUrl
        types
        voltageTypes
    }

    query (
        $communityId: ID
        $coordinates: String
        $postcodeId: ID
        $filter: vnb_FilterInput
        $withCommunity: Boolean = false
        $withCoordinates: Boolean = false
        $withPostcode: Boolean = false
    ) {
        vnb_coordinates(coordinates: $coordinates) @include(if: $withCoordinates) {
            vnbs(filter: $filter) {
                ...vnb_VNB
            }
        }
        vnb_community(id: $communityId) @include(if: $withCommunity) {
            _id
            name
            bbox
            layerUrl
            regions(filter: $filter) {
                ...vnb_Region
            }
            vnbs(filter: $filter) {
                ...vnb_VNB
            }
        }
        vnb_postcode(id: $postcodeId) @include(if: $withPostcode) {
            _id
            name
            code
            bbox
            layerUrl
            regions(filter: $filter) {
                ...vnb_Region
            }
            vnbs(filter: $filter) {
                ...vnb_VNB
            }
        }
    }
    """
    return query


# %%
def execute_query(variables, retry_count=0, max_retries=5):
    url = "https://www.vnbdigital.de/gateway/graphql"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": get_random_user_agent()
    }

    query = get_query_selector()
    payload = {
        "query": query,
        "variables": variables
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            return False
    except Exception as e:
        if retry_count < max_retries:
            print(f"Failed with exception: {e}")
            print(f"Retrying after rotating VPN")

            # Rotate the VPN
            if retry_count % 2 == 0:
                print("Sleeping for 30 seconds before rotating VPN")
                # Disconnect VPN
                terminate_VPN(vpn_settings)
                time.sleep(30)
            try:
                rotate_VPN(vpn_settings)
            except Exception as e:
                print(f"Failed to rotate VPN with exception: {e}")
                return False
            # Try again with incremented retry_count
            return execute_query(variables, retry_count + 1, max_retries)
        else:
            # Exceeded maximum retries
            print(f"Failed after {max_retries} retries.")
            return False


method = input("Enter the method to use (scraperAPI or nordVPN): ")

if (method == "scraperAPI"):
    scraperAPI_key = input("Enter the ScraperAPI key: ")
else:
    from nordvpn_switcher import initialize_VPN, rotate_VPN, terminate_VPN
    vpn_settings = initialize_VPN()


def execute_query_scraperAPI(variables, retry_count=0, max_retries=5):
    url = "https://api.scraperapi.com"

    original_url = "https://www.vnbdigital.de/gateway/graphql"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": get_random_user_agent()
    }

    params = {
        "api_key": scraperAPI_key,
        "url": original_url
    }

    query = get_query_selector()
    payload = {
        "query": query,
        "variables": variables
    }

    try:
        response = requests.post(url, headers=headers,
                                 params=params, json=payload)
        if response.status_code == 200:
            return response.json()
        else:
            return False
    except Exception as e:
        print(f"Failed with exception: {e}")
        return False


if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser(
        description="Get the coordinates from the VNB Digital")
    parser.add_argument("--file", help="The file with the coordinates",
                        default="grid_connection_points_original.xlsx")
    parser.add_argument("--loglevel", help="The log level", default="INFO")

    # Parse the arguments
    args = parser.parse_args()

    print(args.loglevel)

    logging.basicConfig(level=logging.getLevelName(
        args.loglevel), filename="getCoordinates.log", filemode="w", format="%(asctime)s - %(levelname)s - %(message)s")

    # Load the data from the XLSX
    data = pd.read_excel("grid_connection_points.xlsx")
    data = data.reset_index(drop=True)

    # The coordinates are in column Longiture and Latitude
    # We will iterate over the rows and get the coordinates
    i = 1

    # dataSet = {}

    data['Coordinates'] = data.apply(
        lambda row: f"{convert_to_regular_coordinates(row['Longitude'], row['Latitude'])}", axis=1)

    data["vnb-digital"] = ""
    data["voltage-levels"] = ""
    # rotate_VPN(vpn_settings)
    with open("result.json", "r", encoding="utf-8") as f:
        dataSet = json.load(f)

    for index, row in data.iterrows():

        if (row["id"] in dataSet):
            i += 1
            print(f"Already processed {row['id']}")
            continue
        lon = row["Longitude"]
        lat = row["Latitude"]
        # print(lon, lat)
        # print(get_vnb_from_vnb_digital(lon, lat))
        x, y = convert_to_regular_coordinates(lon, lat)
        # Add a new column 'Coordinates' combining 'Longitude' and 'Latitude'
        variables = {
            "filter": {
                "onlyNap": False,
                "voltageTypes": [
                    "Niederspannung",
                    "Mittelspannung"
                ],
                "withRegions": False
            },
            "coordinates": f"{x},{y}",
            "withCoordinates": True
        }
        if (method == "scraperAPI"):
            result = execute_query_scraperAPI(variables)
        else:
            result = execute_query(variables)

        print(i)

        if (result):
            vnbs = []
            vnb_names = []
            voltage_levels = []

            if "vnbs" not in result["data"]["vnb_coordinates"]:
                print("No VNBs found")
                continue

            for vnb in result["data"]["vnb_coordinates"]["vnbs"]:
                vnb_names.append(vnb["name"])
                # row["vnb-digital"] = row["vnb-digital"].join(vnb["name"])
                current_vnb = {
                    "id": vnb["_id"],
                    "name": vnb["name"],
                    "voltageTypes": vnb["voltageTypes"],
                    "types": vnb["types"],
                }
                vnbs.append(current_vnb)
                voltage_levels.append(f"{vnb['voltageTypes']}")

            row["vnb-digital"] = ", ".join(vnb_names)
            row["voltage-levels"] = ", ".join(voltage_levels)

            data.at[index, "vnb-digital"] = row["vnb-digital"]
            data.at[index, "voltage-levels"] = row["voltage-levels"]
            dataSet[row["id"]] = vnbs

        with open("result.json", "w", encoding="utf-8") as f:
            json.dump(dataSet, f, ensure_ascii=False, indent=4)
        if (i % 50 == 0):
            if (method != "scraperAPI"):
                rotate_VPN(vpn_settings)

            # Write the dataframe to an updated file
            with pd.ExcelWriter("updatedData.xlsx", engine="openpyxl") as writer:
                data.to_excel(writer, index=False)

        # print(convert_to_regular_coordinates(lon, lat))
        i += 1
        # Get random sleep time between 1 and 5 seconds
        # sleep_time = 0 + (2*random.random())
        # if (i % 20 == 0):
        #     time.sleep(4 + (2*random.random()))
        # time.sleep(sleep_time)

    with open("result.json", "w", encoding="utf-8") as f:
        json.dump(dataSet, f, ensure_ascii=False, indent=4)

    # Write the dataframe to an updated file
    with pd.ExcelWriter("finalData.xlsx", engine="openpyxl") as writer:
        data.to_excel(writer, index=False)
