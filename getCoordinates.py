import requests
import pandas as pd
import json
import time
import logging
import argparse
from pyproj import Proj, Transformer
import random

# %%


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
def execute_query(variables):
    url = "https://www.vnbdigital.de/gateway/graphql"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    query = get_query_selector()
    payload = {
        "query": query,
        "variables": variables
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        return False


if __name__ == "__main__":

    # Create the parser
    parser = argparse.ArgumentParser(
        description="Get the coordinates from the VNB Digital")
    parser.add_argument("--file", help="The file with the coordinates",
                        default="grid_connection_points.xlsx")
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
    dataSet = {}

    data['Coordinates'] = data.apply(lambda row: f"{convert_to_regular_coordinates(
        row['Longitude'], row['Latitude'])}", axis=1)

    data["vnb-digital"] = ""
    data["voltage-levels"] = ""

    for index, row in data.iterrows():
        print(i)
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
        result = execute_query(variables)

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
                voltage_levels.append(f"{vnb["voltageTypes"]}")

            row["vnb-digital"] = ", ".join(vnb_names)
            row["voltage-levels"] = ", ".join(voltage_levels)

            data.at[index, "vnb-digital"] = row["vnb-digital"]
            data.at[index, "voltage-levels"] = row["voltage-levels"]
            dataSet[row["id"]] = vnbs

        if (i % 2 == 0):
            with open("result.json", "w", encoding="utf-8") as f:
                json.dump(dataSet, f, ensure_ascii=False, indent=4)

            # Write the dataframe to an updated file
            with pd.ExcelWriter("updatedData.xlsx", engine="openpyxl") as writer:
                data.to_excel(writer, index=False)

        # print(convert_to_regular_coordinates(lon, lat))
        i += 1
        if i == 20:
            url = f"https://www.google.de/maps/@{x},{y},15z"
            print(url)
            break

        # Get random sleep time between 1 and 5 seconds
        sleep_time = 1 + (4 * random.random())
        time.sleep(sleep_time)
