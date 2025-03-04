import sys
import os
import re

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from datetime import datetime
from utils import *
import pymongo
import json
from constant import *
from pymongo import UpdateOne


def get_state_from_address(address):
    state = ""
    if not address:
        return state
    try:
        tokens = re.split(r"[,\s]+", address)
        for tok in reversed(tokens):
            if state_code.get(tok):
                state = state_code[tok]
                break
        if not state:
            return ""
    except:
        pass
    return state


def get_city_from_address(address: str, cityDict=stateCodeTOcityDict):
    try:
        stateCodeList = cityDict.keys()
        state_code_found = None
        city_found = None
        address_parts = [part.strip() for part in address.split(",")]

        # Look for a state code in the address
        for state_code in stateCodeList:
            if state_code in address:
                state_code_found = state_code
                break

        if state_code_found:
            # Check for city in mapping for the found state code
            pre_state_text = address.split(f", {state_code_found}")[0]
            pre_state_parts = [part.strip() for part in pre_state_text.split(",")]

            for possible_city in stateCodeTOcityDict[state_code_found]:
                for part in pre_state_parts:
                    if possible_city.strip() == part.strip():
                        city_found = possible_city
                        break
                if city_found:
                    break

        # Fallback if no city is found
        if not city_found and state_code_found:
            city_found = pre_state_parts[-1] if pre_state_parts else None

        # Fallback to the second part of the address if parsing fails
        if not city_found and len(address_parts) > 1:
            city_found = address_parts[1]

        return city_found.strip()
    except:
        return address.split(",")[1].strip()

    return UpdateOne({"url_hash": hash}, {"$set": temp}, upsert=True)


def sanity_hours(hours):
    if "closed" in hours:
        hours = "closed"
    elif hours == "open 24 hours":
        hours = "00:00 to 23:59"
    else:
        hours_arr = hours.split("to")
        if (
            "am" not in hours_arr[0].strip()
            and "pm" not in hours_arr[0]
            and ":" in hours_arr[0]
        ):
            open_time = convert_to_24h(hours_arr[0].strip() + " pm", True)
        elif "am" not in hours_arr[0] and "pm" not in hours_arr[0]:
            open_time = convert_to_24h(hours_arr[0].strip() + " pm")
        elif ":" in hours_arr[0]:
            open_time = convert_to_24h(hours_arr[0].strip(), True)
        else:
            open_time = convert_to_24h(hours_arr[0].strip())

        if ":" in hours_arr[1]:
            close_time = convert_to_24h(hours_arr[1].strip(), True)
        else:
            close_time = convert_to_24h(hours_arr[1].strip())
        hours = open_time + " to " + close_time
    return hours


def convert_to_24h(time_str, colon=False):
    format_str = "%I:%M %p" if colon else "%I %p"
    time_obj = datetime.strptime(time_str, format_str)
    return time_obj.strftime("%H:%M")


def get_format_data(db, google_id):
    data = {}
    data = db.google_maps_format.find_one(
        {"google_id": google_id},
        # {
        #     "_id": 0,
        #     "name": 1,
        #     "google_id": 1,
        #     "address": 1,
        #     "city": 1,
        #     # "phone": 1,
        #     "phone_no": "$phone",
        #     "website": 1,
        #     # "category": 1,
        #     "category_cuisine_google": "$category",
        #     "opening_hours": 1,
        #     "extracted_dishes": 1,
        #     "img_url": 1,
        #     "amenties": 1,
        #     "lat": 1,
        #     "long": 1,
        #     "published": 1,
        #     "topmenus_published_at": 1,
        #     "topmenus_republished_at": 1,
        # },
    )
    if data:
        return data
    data = db.google_maps_4.find_one({"google_id": google_id})
    formatted_data = {}
    print(f"Formating Restaurant :: {data['google_id']}")
    try:
        basic_info = data["basic_info"]
        # formatted_data["_id"] = str(data["_id"])
        formatted_data["name"] = basic_info["name"]
        formatted_data["img_url"] = basic_info["img_url"]
        formatted_data["category_cuisine_google"] = basic_info["category"]
        formatted_data["city"] = (
            basic_info["city"]
            if basic_info.get("city")
            else get_city_from_address(
                basic_info["info_block"]["Address"].strip()
                if basic_info["info_block"].__contains__("Address")
                else ""
            )
        )
        formatted_data["state"] = (
            basic_info["state"]
            if basic_info.get("state")
            else get_state_from_address(
                basic_info["info_block"]["Address"].strip()
                if basic_info["info_block"].__contains__("Address")
                else ""
            )
        )
        formatted_data["state_postal_abb"] = us_states_dict.get(
            formatted_data["state"].lower(), None
        )
        formatted_data["lat"] = float(basic_info["lat"])
        formatted_data["long"] = float(basic_info["long"])
        formatted_data["address"] = (
            basic_info["info_block"]["Address"].strip()
            if basic_info["info_block"].__contains__("Address")
            else ""
        )
        formatted_data["phone_no"] = (
            basic_info["info_block"]["Phone"].strip()
            if basic_info["info_block"].__contains__("Phone")
            else ""
        )
        formatted_data["website"] = ""
        if basic_info["info_block"].__contains__("website"):
            formatted_data["website"] = basic_info["info_block"]["website"]
        amenties = []
        if data.__contains__("about") and data["about"]:
            for key, value in data["about"].items():
                try:
                    if key == "accessibility" and len(data["about"][key]) > 0:
                        amenties.append("Wheelchair Accesible")
                    else:
                        for d in value:
                            amenties.append(d)
                except IndexError:
                    pass

        formatted_data["amenties"] = list(set(amenties))

        opening_hours = {}
        if basic_info["opening_hours"] is not None:
            opening_hours_res = (
                basic_info["opening_hours"]
                .replace(". Hide open hours for the week", "")
                .strip()
            )
            if opening_hours_res != "":
                opening_hours_res = opening_hours_res.split(";")
                if len(opening_hours_res) > 0:
                    for d in opening_hours_res:
                        day_hours = d.split(",")
                        day = day_hours[0].lower().strip()
                        day = day.split()[0]
                        hours = sanity_hours(day_hours[1].lower().strip())
                        opening_hours[day] = hours
        formatted_data["opening_hours"] = opening_hours

        # write_to_json_file(formatted_data)
        return formatted_data

    except Exception as e:
        print(f"Error in format data:: {data['google_id']}", e)
