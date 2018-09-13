import requests as req
import os
import re
import xml.etree.ElementTree as XMLParser
from dotenv import load_dotenv
from pymongo import MongoClient, ReplaceOne, GEOSPHERE
from pymongo.errors import ConnectionFailure, OperationFailure

# setup
load_dotenv("../.env")

# connect to MLab database
client = MongoClient(
    "mongodb://{DB_USER}:{DB_PASSWORD}@ds040837.mlab.com:40837/kleiderspenden".format(**os.environ))
db = client["kleiderspenden"]

# check if connection was successful
try:
    db.command("ismaster")
    print("Successfully connected to '{0}'".format(db.name))
except (ConnectionFailure, OperationFailure) as e:
    print("Failed to connect to db '{0}': {1}".format(db.name, e))


# get XML data from AWB for Kleidercontainer and Kleiderkammern
chambers = req.get("https://www.awbkoeln.de/geodaten/kleiderkammern/")
containers = req.get("https://www.awbkoeln.de/geodaten/altkleider/")

# parse XML
docs = list(XMLParser.fromstring(x.content) for x in [chambers, containers])

# put into dict
raw_data_list = []

for root in docs:
    for child in root:
        raw_data = {}
        for elt in child:
            raw_data[elt.tag] = elt.text
        raw_data_list.append(raw_data)

# clean up of coordinates
for elt in raw_data_list:
    # remove leading "," in some coordinates
    elt["coordinates"] = re.sub(r"^,", "", elt["coordinates"])
    # replace middle "," with "_" as value separator
    elt["coordinates"] = re.sub(r"(\d{3}),", r"\1_", elt["coordinates"])
    # replace "," as decimal point in some coordinates
    elt["coordinates"] = re.sub(r",", ".", elt["coordinates"])


# TODO: clean up phone numbers into unified format
# TODO: parse opening hours

category_dict = {
    "Gemeinnützige Einrichtung": "charity",
    "Wertstoff-Center": "recycling",
    "Altkleidercontainer": "container"
}

# restructure data
data_list = list({
    "uid": x["uid"],
    "category": {
        "name": category_dict[x["type"]],
        "desc": x["type"]
    },
    "name": x["name"],
    "location": {
        "street": x["street"],
        "zipcode": x["zipcode"],
        "city": x["city"].lower() if x["city"] is not None else x["city"],
        "district": x["district"].lower() if x["district"] is not None else x["district"],
        "area": x["area"].lower() if x["area"] is not None else x["area"],
        "gps_location": {
            "type": "Point",
            "coordinates": [
                float(x["coordinates"].split("_")[1]),
                float(x["coordinates"].split("_")[0])
            ]
        }
    },
    "openinghours": x["openinghours"],
    "details": {
        "phone": x["phone"],
        "url": x["www"],
        "maptitle": x["maptitle"],
        "locationname": x["locationname"]
    }
} for x in raw_data_list)

queries = list(ReplaceOne({"uid": x["uid"]},
                          x, upsert=True) for x in data_list)
db_result = db["sites"].bulk_write(queries)
print("Matched {0} and upserted {1} documents".format(
    db_result.matched_count, db_result.upserted_count))

print("Creating GeoIndex")
db["sites"].create_index([("location.gps_location", GEOSPHERE)])

print("Done")
