# Description:
# This script inserts job listing posts into a WordPress site using data from a MongoDB database.
# It processes the data, formats business hours, and inserts posts with the corresponding metadata.

# Dependencies:
# - argparse: For parsing command-line arguments.
# - concurrent.futures: For handling concurrency with ThreadPoolExecutor.
# - datetime: For working with dates and times.
# - re: For regular expressions.
# - pymysql: For MySQL database operations.
# - pymongo: For MongoDB operations.

# Arguments:
# - --name: (str) Enter the restaurant name, separating each with a comma. Defaults to an empty string.
# - --limit: (str) Enter the limit. Defaults to 0.

# Response Type:
# The script does not explicitly return a response. It performs database operations to insert data into WordPress.

import pymongo
import concurrent.futures
import datetime
import traceback
import phpserialize
from constant import *
import pymysql
import unicodedata
import re
import pytz
import random
import string
from collections import defaultdict
import argparse
import traceback
import sys
import os
import io
import requests
import magic
import paramiko
from datetime import timedelta, datetime
from PIL import Image
from io import BytesIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from constant import *
from dotenv import load_dotenv
from logger import ErrorLogger

load_dotenv(override=True)
from os import getenv
import hashlib
from utils import *

slugSet = set()
WORDPRESS_LIMIT = int(getenv("WORDPRESS_LIMIT"))

ds_conn = pymongo.MongoClient(getenv("ONE_MENUS_PRODUCT"))
ds_db = ds_conn["topmenus_product"]

dev_conn = pymongo.MongoClient(getenv("GOOGLE_MAPS_DB_URI"))
dev_db = dev_conn["topmenus_crawling"]

# mysql_conn = pymysql.connect(host='5.189.184.167',user="admin_topmenus",passwd="4NxJfLvMao",db="admin_topmenus", cursorclass=pymysql.cursors.DictCursor)
# cursor = mysql_conn.cursor()

current_date = datetime.now(tz=pytz.UTC)

contabo_private_key_path = "./id_rsa"
contabo_host = getenv("WP_HOST")
contabo_port = getenv("WP_PORT")
contabo_username = getenv("WP_USER")
contabo_remote_path = (
    "/home/admin/web/top-menus.com/public_html/wp-content/uploads/"
    + current_date.strftime("%Y")
    + "/"
    + current_date.strftime("%m")
    + "/"
)
contabo_password = getenv("WP_PASS")
ssh = paramiko.SSHClient()
ssh.load_system_host_keys()  # Load known_hosts
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Auto-accept unknown hosts
# Connect using the SSH agent (existing keys)
ssh.connect("174.138.54.30", username="root")
# ssh.connect("174.138.54.30", username="manish_goyal", password="xzmgcLoXx4jYINP6")
print("✅ Connected to server successfully!")

# transport = paramiko.Transport((contabo_host, int(contabo_port)))
# transport.connect(username=contabo_username, password=contabo_password)
# key = paramiko.RSAKey.from_private_key_file(contabo_private_key_path)
# transport.connect()
# transport.connect(username=contabo_username, pkey=key)
# sftp = paramiko.SFTPClient.from_transport(transport)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

logger = ErrorLogger(
    log_to_terminal=True, log_file_name="publish-wordpress", log_to_file=True
)


def process_data(data):

    if data:
        for d in data:
            try:
                main_data = get_format_data(dev_db, d["google_id"])
                # d.update(main_data)
                # print(d)
                # dev_db.format_temp.insert_one(d)
                # continue
                logger.info(f"Publishing restaurant - {d['_id']}")
                # mysql_conn = pymysql.connect(
                #     host="174.138.54.30",
                #     user="root",
                #     passwd="",
                #     db="wordpress",
                #     cursorclass=pymysql.cursors.DictCursor,
                # )
                mysql_conn = pymysql.connect(
                    host="174.138.54.30",
                    user="manish_goyal",
                    password="xzmgcLoXx4jYINP6",
                    database="wordpress",
                    cursorclass=pymysql.cursors.DictCursor,
                )
                print("connected to sql")
                cursor = mysql_conn.cursor()
                # continue
                if d.__contains__("long"):
                    d["long"] = str(d["long"])  # .replace('-', '')

                if d.__contains__("lat"):
                    d["lat"] = str(d["lat"])  # .replace('-', '')

                if not d.get("phone_no"):
                    d["phone_no"] = ""

                post_title = d["name"].replace("'", "")
                # Eg: big-city-wings-636832-menu
                post_name = generate_unique_slug(
                    d["_id"],
                    d["name"]
                    .replace("-", "")
                    .replace(".", "")
                    .replace(",", "")
                    .replace(";", "")
                    .replace(":", ""),
                )
                post_name = f"{post_name}-menu"
                print("post_name -", post_name)

                content = generate_content(d, post_title)
                logger.info(msg=content)
                business_hours = ""
                if (
                    "opening_hours" in d
                    and not (isinstance(d["opening_hours"], float))
                    and d["opening_hours"]
                ):
                    business_hours = generate_business_hours(d["opening_hours"])

                # CHECK IF POST ALREADY EXISTS
                check_with_quote = d["name"].strip()
                check_without_quote = d["name"].replace("'", "").strip()

                # check_sql = "SELECT wp.ID, wp.post_name, wp.post_title, wp.post_content, wpm.meta_key, wpm.meta_value FROM wp_posts as wp join wp_postmeta as wpm on wp.ID = wpm.post_id WHERE wp.post_title = %s OR wp.post_title = %s AND wp.post_status='publish'"
                # check_sql = """
                #             SELECT wp.ID, wp.post_name, wp.post_title, wp.post_content, wpm.meta_key, wpm.meta_value
                #             FROM wp_posts as wp
                #             JOIN wp_postmeta as wpm on wp.ID = wpm.post_id
                #             WHERE (wp.post_title = %s OR wp.post_title = %s) AND wp.post_status='publish'
                #             ORDER BY post_date_gmt DESC"""
                check_sql = """
                            SELECT wp.ID, wp.post_name, wp.post_title, wp.post_content, wpm.meta_key, wpm.meta_value
                            FROM wp_posts as wp
                            JOIN wp_postmeta as wpm on wp.ID = wpm.post_id
                            WHERE (LOWER(wp.post_title) = LOWER(%s) OR LOWER(wp.post_title) = LOWER(%s)) AND wp.post_status='publish'
                            ORDER BY post_date_gmt DESC"""
                # Execute the check query
                cursor.execute(check_sql, (check_with_quote, check_without_quote))
                result = cursor.fetchall()

                # if result is found then check if same address already exists
                if result:
                    update = False
                    lt_address = None
                    thumbnail_status = False
                    for row in result:

                        if row["meta_key"] == "_lt_address":
                            lt_address = row["meta_value"]
                        if (
                            lt_address
                            and lt_address.replace(", United States", "").strip()
                            == d["address"].replace(", United States", "").strip()
                        ):
                            update = True
                            post_id = row["ID"]
                            if row["meta_key"] == "_thumbnail_id":
                                thumbnail_status = True
                            break

                    if update:

                        update_sql = (
                            "update wp_posts set post_content= %s where ID = %s"
                        )
                        cursor.execute(update_sql, (content, post_id))

                        # TODO:
                        # write to update lat long
                        update_meta(cursor, d, post_id)

                        # update meta data
                        update_meta_data(
                            cursor, post_id, post_title, post_name, d, business_hours
                        )
                        print(f"{post_title} - {post_id} - updated")

                        # if not thumbnail_status:
                        #     try:
                        #         insert_featured_image(d, post_id, post_title, post_name)
                        #     except:
                        #         logger.info(msg="Error in featured image")

                    else:
                        last_insert_id = insert_post(
                            cursor, d, post_title, post_name, content, business_hours
                        )
                        print(f"{post_title}  -  {last_insert_id} - inserted")
                        try:
                            insert_featured_image(
                                cursor, d, last_insert_id, post_title, post_name
                            )
                        except:
                            logger.info(msg="Error in featured image")
                else:
                    last_insert_id = insert_post(
                        cursor, d, post_title, post_name, content, business_hours
                    )
                    print(f"{post_title}  -  {last_insert_id} - inserted")
                    # image create code
                    try:
                        insert_featured_image(
                            cursor, d, last_insert_id, post_title, post_name
                        )
                    except Exception as e:
                        logger.info(msg=f"Error in featured image - {e}")
                mysql_conn.commit()
                mysql_conn.close()

                update = {
                    "published": True,
                }

                if d.get("published"):
                    update["topmenus_republished_at"] = datetime.now()
                else:
                    update["topmenus_published_at"] = datetime.now()

                ds_db.onemenus_ocr.update_one({"_id": d["_id"]}, {"$set": update})
                logger.info(msg=f"Published successfully - {d['_id']}")
            except Exception as e:
                traceback.print_exc()
                logger.exception(msg=f'error in main - {d["name"]}')
                update_data = {
                    "$set": {
                        "id": d["_id"],
                        "name": d["name"],
                        # 'type': type(e).__name__,
                        "error": str(e),
                        "message": traceback.format_exc(),
                        "created_at": current_date,
                    }
                }
                ds_db.publish_errors.update_one(
                    {"_id": d["_id"]}, update_data, upsert=True
                )
                print(d["name"])
                print(e)


def ensure_remote_dir(sftp, remote_dir):
    try:
        sftp.chdir(remote_dir)
    except IOError:
        dirs = remote_dir.split("/")
        path = ""
        for dir in dirs:
            path = f"{path}/{dir}"
            try:
                sftp.chdir(path)
            except IOError:
                sftp.mkdir(path)
                sftp.chdir(path)


def insert_featured_image(cursor, data, post_id, post_title, post_name):
    # donwnload image
    address = ""
    city = ""
    if data["address"] != "":
        address_arr = data["address"].split(",")
        address = address_arr[0].strip()
        city = address_arr[1].strip()

    cuisine = (
        data.get("category_cuisine_google", "")
        .replace("restaurant", "")
        .replace("restaurants", "")
        .replace("Restaurant", "")
        .replace("Restaurants", "")
        .strip()
    )
    post_title = (
        post_title
        + " restaurant "
        + address
        + " "
        + city
        + " latest menu "
        + cuisine
        + " "
        + current_date.strftime("%Y")
    )
    if cuisine != "":
        post_content = (
            post_title
            + " restaurant is in Austin Texas having a menu primarily catering to "
            + cuisine
            + " as the primary cuisine"
        )
        img_alt_text = post_title + " having " + cuisine + " food menu"
    else:
        post_content = post_title + " restaurant is in Austin Texas"
        img_alt_text = post_title + "having food menu"

    post_excerpt = (
        post_title
        + " restaurant ambience in "
        + city
        + " Texas having a menu primarily catering to "
        + cuisine
        + " | Source : Google"
    )
    post_name = slugify(
        post_title.replace("-", "")
        .replace(".", "")
        .replace(",", "")
        .replace(";", "")
        .replace(":", "")
    )

    guid = (
        "https://top-menus.com/wp-content/uploads/"
        + current_date.strftime("%Y")
        + "/"
        + current_date.strftime("%m")
        + "/"
    )

    min_size = (200, 200)
    if data["img_url"] is not None:
        img_url = data["img_url"]
        base_url = img_url.split("=")[0]
        # Append the new parameters
        modified_url = base_url + "=s1360-w1360-h1020"

        response = requests.get(modified_url.strip(), verify=False)
        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))

            # Check if the image meets the minimum size requirements
            if img.size[0] < min_size[0] or img.size[1] < min_size[1]:
                img = img.resize(min_size, Image.ANTIALIAS)
            # logger.info(msg=f"image - {img.size[0]} - {img.size[1]}")
            mime_type = __get_image_extension(
                magic.from_buffer(response.content, mime=True)
            )

            filename = f"{str(post_name)}.{mime_type}"
            fileSizes = ["600x540", "560x370", "768x576", "175x175", "1024x768", ""]

            ensure_remote_dir(sftp, contabo_remote_path)

            # Define the remote file path
            db_file_path = ""
            for fs in fileSizes:

                if fs != "":
                    remote_file_path = (
                        contabo_remote_path + f"{str(post_name)}-{fs}.{mime_type}"
                    )

                else:
                    remote_file_path = (
                        contabo_remote_path + f"{str(post_name)}.{mime_type}"
                    )
                db_file_path = remote_file_path.replace(
                    "/home/admin/web/top-menus.com/public_html/wp-content/uploads/", ""
                )
                local_file_path = remote_file_path.replace(contabo_remote_path, "")

                # Save the image content to a temporary file
                local_temp_file = "featured_images/" + local_file_path
                os.makedirs(os.path.dirname(local_temp_file), exist_ok=True)
                img.save(local_temp_file)

                # Upload the temporary file to the remote server
                sftp.put(local_temp_file, remote_file_path)

            guid = guid + filename
            post_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
            insert_sql = "insert into wp_posts (post_author,post_date,post_date_gmt,post_content, post_title, post_excerpt, post_status, post_name, post_modified, post_modified_gmt, post_parent, guid, post_type, post_mime_type) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s)"
            res = cursor.execute(
                insert_sql,
                (
                    22,
                    post_date,
                    post_date,
                    post_content,
                    post_title,
                    post_excerpt,
                    "inherit",
                    post_name,
                    post_date,
                    post_date,
                    post_id,
                    guid,
                    "attachment",
                    "image/jpeg",
                ),
            )
            last_insert_id = cursor.lastrowid

            meta_object = {}
            meta_object["_wp_attached_file"] = db_file_path
            meta_object["_wp_attachment_image_alt"] = img_alt_text

            meta_data = ""
            size = os.path.getsize(local_temp_file)
            meta_data += 'a:6:{s:5:"width";'
            meta_data += f'i:{img.size[0]};s:6:"height";i:{img.size[1]};s:4:"file";'
            meta_data += f's:{len(db_file_path)}:"{db_file_path}";'
            meta_data += f's:8:"filesize";i:{size};s:5:"sizes";'
            meta_data += "a:6:{"
            sizes = {
                "medium": "600x540",
                "large": "1024x768",
                "thumbnail": "175x175",
                "medium_large": "768x576",
                "post-thumbnail": "560x370",
                "lestin_medium": "600x540",
            }
            for index, value in sizes.items():
                meta_data += f's:{len(index)}:"{index}";'
                meta_data += 'a:5:{s:4:"file";'
                resolutions = value.split("x")
                sz = f"{post_name}-{value}.{mime_type}"
                meta_data += f's:{len(sz)}:"{sz}";'
                meta_data += (
                    f's:5:"width";i:{resolutions[0]};s:6:"height";i:{resolutions[1]};'
                )
                # meta_data += f's:{len(sz)}:"{sz}";'
                meta_data += (
                    f's:9:"mime-type";s:10:"image/jpeg";s:8:"filesize";i:{size};'
                )
                meta_data += "}"

            meta_data += '}s:10:"image_meta";a:12:{s:8:"aperture";s:1:"0";s:6:"credit";s:0:"";s:6:"camera";s:0:"";s:7:"caption";s:0:"";s:17:"created_timestamp";s:1:"0";s:9:"copyright";s:0:"";s:12:"focal_length";s:1:"0";s:3:"iso";s:1:"0";s:13:"shutter_speed";s:1:"0";s:5:"title";s:0:"";s:11:"orientation";s:1:"0";s:8:"keywords";a:0:{}}}'
            meta_object["_wp_attachment_metadata"] = meta_data

            for index, value in meta_object.items():
                try:
                    sql2 = """insert into wp_postmeta (post_id,meta_key,meta_value) values(%s, %s, %s)"""
                    cursor.execute(sql2, (last_insert_id, index, value))
                except:
                    logger.debug(msg="Error in featured image meta data::")

            sql2 = """insert into wp_postmeta (post_id,meta_key,meta_value) values(%s, %s, %s)"""
            cursor.execute(sql2, (post_id, "_thumbnail_id", last_insert_id))


def __get_image_extension(mime_type):
    valid_extensions = {
        "image/bmp": "bmp",
        "image/gif": "gif",
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/svg+xml": "svg",
        "image/tiff": "tiff",
        "image/x-icon": "ico",
        "image/vnd.microsoft.icon": "ico",
        "image/vnd.wap.wbmp": "wbmp",
        "image/heic": "heic",
        "image/heif": "heif",
        "image/heif-sequence": "heif",
        "image/jp2": "jp2",
        "image/jxr": "jxr",
        "image/avif": "avif",
    }
    extension = valid_extensions.get(mime_type)
    if extension is None:
        return "png"
    return extension


def generate_content(d, post_title):
    loc_code = f" ({d.get('city', '')}, {d.get('state_postal_abb', '')}) "

    content = ""
    content += "<h1>{}{}Menu:</h1>".format(d["name"].replace("'", ""), loc_code)
    content += '<div class="menu_div">'
    content += '<table dir="ltr" cellspacing="0" cellpadding="0"><colgroup><col width="815" /><col width="100" /></colgroup><tbody>'

    grouped_menu = defaultdict(list)
    menu_name = menu_desc = menu_price = menu_type = ""
    for dt in d["extracted_dishes"]:
        if isinstance(dt, list):
            continue
        if dt.get("Type") and dt.get("Type", "").strip() != "":
            type = dt["Type"]
            grouped_menu[type].append(dt)
        else:
            grouped_menu["More Food, Beverages & Desserts"].append(dt)

    for type, items in grouped_menu.items():
        if type != "More Food, Beverages & Desserts":
            content += '<tr class="rest_item">'
            type = type.replace("'", "").strip()
            content += f"<td><h5>{type}</h5></td>"
            for item in items:
                content += '<tr class="rest_name">'
                name = item["Name"].strip().replace("'", "") if item.get("Name") else ""
                content += f"<td><strong>{name}</strong></td>"
                price = (
                    item["Price"].replace("'", "").strip() if item.get("Price") else ""
                )
                content += f"<td>{price}</td>"
                content += '</tr><tr class="rest_desc">'
                if item.get("Desc") and item["Desc"].strip() != "":
                    desc = (
                        item["Desc"].strip().replace("'", "")
                        if item.get("Desc")
                        else ""
                    )
                    content += '<tr class="rest_desc">'
                    content += f"<td>{desc}</td>"
                    content += "<td></td></tr>"
            content += "</tr>"

    if (
        "More Food, Beverages & Desserts" in grouped_menu
        and len(grouped_menu["More Food, Beverages & Desserts"]) > 0
    ):
        content += '<tr class="rest_item">'
        content += f"<td><h5>More Food, Beverages & Desserts</h5></td>"
        for item in grouped_menu["More Food, Beverages & Desserts"]:
            content += '<tr class="rest_name">'
            name = (
                item.get("Name", "").strip().replace("'", "")
                if item.get("Name")
                else ""
            )
            content += f"<td><strong>{name}</strong></td>"
            price = (
                item.get("Price").replace("'", "").strip() if item.get("Price") else ""
            )
            content += f"<td>{price}</td>"
            if item.get("Desc") and item.get("Desc", "").strip() != "":
                content += '<tr class="rest_desc">'
                desc = item["Desc"].strip().replace("'", "")
                content += f"<td>{desc}</td>"
                content += "<td></td></tr>"
        content += "</tr>"

    content += "</tbody></table>"
    content += "</div>"
    content += '<div class="rest_div"></div>'
    content += f'<h6>{post_title} restaurant menu is provided by <a href="https://www.allmenus.com/" target="_blank" rel="noopener">top-menus.com</a>.</h6>DISCLAIMER: Information shown may not reflect recent changes. Check with this restaurant for current pricing and menu information. A listing on top-menus.com does not necessarily reflect our affiliation with or endorsement of the listed restaurant, or the listed restaurant’s endorsement of top-menus.com. Please tell us by <a href="https://top-menus.com/contact/" target="_blank" rel="noopener">clicking here</a> if you know that any of the information shown is incorrect. For more information, please read our <a href="https://top-menus.com/terms-and-conditions/" target="_blank" rel="noopener">Terms and Conditions</a>.'
    return content


def generate_business_hours(opening_hours_):
    opening_hours = {
        re.sub(r"\s*\(.*\)", "", day.strip()): time
        for day, time in opening_hours_.items()
    }
    business_hours = "a:7:{"
    for index, day in WEEK_DAYS_OPENING_HOURS.items():
        business_hours += f's:3:"{index}";a:2:' + "{"
        if opening_hours[day].lower() == "closed":
            business_hours += f's:6:"option";s:9:"close_day";s:3:"hrs";a:1:' + "{"
            business_hours += "i:0;a:2:{"
            business_hours += f's:4:"from";s:5:"11:00";s:2:"to";s:5:"20:00";'
        else:
            business_hours += f's:6:"option";s:12:"custom_hours";s:3:"hrs";a:1:' + "{"
            business_hours += "i:0;a:2:{"
            if opening_hours[day].lower() == "open 24 hours":
                open_time = close_time = "12:00"
            else:
                timing = opening_hours[day].split("to")
                # open_time =

                # if 'am' not in timing[0] and 'pm' not in timing[0] and ':' in timing[0]:
                #     open_time = convert_to_24h(timing[0].strip() + ' pm', True)
                # elif 'am' not in timing[0] and 'pm' not in timing[0] :
                #     open_time = convert_to_24h(timing[0].strip() + ' pm')
                # elif ':' in timing[0]:
                #     open_time = convert_to_24h(timing[0].strip(), True)
                # else:
                #     open_time = convert_to_24h(timing[0].strip())

                # if ':' in timing[1]:
                #     close_time = convert_to_24h(timing[1].strip(), True)
                # else:
                #     close_time = convert_to_24h(timing[1].strip())
            business_hours += f's:4:"from";s:5:"{timing[0].strip()}";s:2:"to";s:5:"{timing[1].strip()}";'
        business_hours += "}}}"
    business_hours += "}"
    return business_hours


def insert_post(cursor, d, post_title, post_name, content, business_hours):
    ping_status = "closed"
    post_type = "job_listing"
    post_status = "publish"
    post_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
    post_parent = 0
    post_author = 22

    insert_sql = """insert into wp_posts (post_author,post_date,post_date_gmt,post_content, post_title, post_status, ping_status, post_name, post_modified, post_modified_gmt, post_parent, post_type) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    res = cursor.execute(
        insert_sql,
        (
            post_author,
            post_date,
            post_date,
            content,
            post_title,
            post_status,
            ping_status,
            post_name,
            post_date,
            post_date,
            post_parent,
            post_type,
        ),
    )
    last_insert_id = cursor.lastrowid
    meta_object = {}
    meta_object["_lt_address"] = d["address"]
    meta_object["_lt_price_range"] = "inexpensive"
    # meta_object['_lt_map'] = 'a:2:{s:3:"lat";s:10:"30.3460397";s:3:"lng";s:10:"59.2340242";}'
    if "phone_no" in d and not (isinstance(d["phone_no"], float)):
        meta_object["_lt_phone"] = d["phone_no"]
    if "website" in d and not (isinstance(d["website"], float)):
        meta_object["_lt_website"] = d["website"]
    meta_object["_lt_regions"] = 78
    if business_hours != "":
        meta_object["_lt_hours_value"] = business_hours
    # meta_object['_job_expires'] = '2027-12-31'
    meta_object["_lt_map_latitude"] = str(d["lat"]) if d.__contains__("lat") else ""
    # meta_object['_lt_map_longitude'] = str(d['long']).replace('-', '') if d.__contains__('long') else ''
    meta_object["_lt_map_longitude"] = str(d["long"]) if d.__contains__("long") else ""
    map_object = ""
    if d.__contains__("lat") and d.__contains__("long"):
        lat = d["lat"]
        # long = str(d['long']).replace('-', '')
        long = str(d["long"])  # .replace('-', '')
        # map_object += 'a:2:{s:3:"lat";'
        # map_object += f's:10:"{lat}";'
        # map_object += 's:3:"lng";'
        # map_object += f's:10:"{long}";'
        # map_object += '}'
        php_serialized = phpserialize.dumps({"lat": lat, "lng": long})
        map_object += php_serialized.decode("utf-8")
    else:
        map_object = "a:0:{}"
    meta_object["_lt_map"] = map_object
    meta_object["_lt_place_booking"] = (
        'a:3:{s:4:"type";s:4:"info";s:9:"affiliate";a:4:{s:4:"link";s:0:"";s:4:"site";s:0:"";s:6:"link_2";s:0:"";s:6:"site_2";s:0:"";}s:6:"banner";a:1:{s:3:"url";s:0:"";}}'
    )

    rank_title, rank_description, focus_keyword, schema_descp, shortcode = (
        generate_schema(d, post_title, post_name)
    )

    # post_title_fix = post_title.replace('&', '&amp;').replace('@', '&#64;').replace('#', '&#35;')
    # rank_title = post_title_fix + " (Austin, TX) Latest Menu - " + current_date.strftime('%B %Y')
    # cuisine = d['category_cuisine_google'].replace('restaurant', '').replace('restaurants', '').replace('Restaurant', '').replace('Restaurants', '').strip()
    # rank_description = post_title_fix + " | " + d['address']
    # if cuisine != '':
    #     rank_description = rank_description + " | Cuisine: " + cuisine
    # else:
    #     rank_description = rank_description + " | Cuisine: Restaurant"
    # focus_keyword = f"{post_title},{post_title} menu,{post_title} Restaurant Menu,{post_title} menu austin,Best restaurants in austin,{post_title} Menu with prices,{post_title} near me,{post_title} austin,{post_title} kids menu,Top 10 restaurants in austin"
    # if cuisine != '':
    #     focus_keyword = f"{focus_keyword},{cuisine},{cuisine} near me,{cuisine} near me open now,Best {cuisine} near me"
    # else:
    #     focus_keyword = f"{focus_keyword},Restaurants,Restaurants near me,Restaurants near me open now,Best Restaurants near me"

    # meta_object['rank_math_title'] = rank_title
    # meta_object['rank_math_description'] = rank_description
    # meta_object['rank_math_focus_keyword'] = focus_keyword

    # random_code = random.choices(string.ascii_lowercase + string.digits, k=13)
    # random_code = ''.join(random_code)
    # shortcode = "rank_math_shortcode_schema_s-" + random_code
    # schema_descp = ''

    # schema_descp += 'a:12:{s:8:"metadata";a:5:{s:5:"title";s:10:"Restaurant";s:4:"type";s:8:"template";s:9:"shortcode";'
    # schema_descp += f's:{len(random_code) + 2}:"s-{random_code}";s:9:"isPrimary";s:1:"1";s:23:"reviewLocationShortcode";s:24:"[rank_math_rich_snippet]";'
    # schema_descp += '}'
    # schema_descp += 's:5:"@type";s:10:"Restaurant";'
    # schema_descp += f's:4:"name";s:{len(post_title_fix)}:"{post_title_fix}";'
    # schema_descp += f's:11:"description";s:{len(rank_description)}:"{rank_description}";'
    # telephone = ''
    # if d['phone_no'] != '':
    #     telephone = d['phone_no']

    # 's:17:"5404 Menchaca Rd ";s:15:"addressLocality";s:6:"Austin";s:13:"addressRegion";s:5:"Texas";s:10:"postalCode";s:5:"78745";s:14:"addressCountry";s:3:"USA";}s:3:"geo";a:3:{s:5:"@type";s:14:"GeoCoordinates";s:8:"latitude";s:10:"30.3460397";s:9:"longitude";s:10:"59.2340242";}s:25:"openingHoursSpecification";a:0:{}s:13:"servesCuisine";a:0:{}s:7:"hasMenu";s:49:"https://top-menus.com/listings/austin-java-menu-2";s:5:"image";a:2:{s:5:"@type";s:11:"ImageObject";s:3:"url";s:16:"%post_thumbnail%";}}'
    # schema_descp += f's:9:"telephone";s:{len(telephone)}:"{telephone}";'
    # schema_descp += 's:10:"priceRange";s:1:"$";s:7:"address";a:6:{s:5:"@type";s:13:"PostalAddress";s:13:"streetAddress";'
    # address = ''
    # pin_code = ''
    # city = ''
    # if d['address'] != '':
    #     address_arr = d['address'].split(',')
    #     address = address_arr[0].strip()
    #     city = address_arr[1].strip()
    #     pin_code = address_arr[2].replace('TX', '').strip()

    # lat = d['lat'] if d.__contains__('lat') else ''
    # # long = d['long'].replace('-', '').strip()
    # long = d['long'].replace('-', '') if d.__contains__('long') else ''
    # 's:7:"hasMenu";s:49:"https://top-menus.com/listings/austin-java-menu-2";s:5:"image";a:2:{s:5:"@type";s:11:"ImageObject";s:3:"url";s:16:"%post_thumbnail%";}}'
    # website_url = 'https://top-menus.com/listings/' + post_name
    # schema_descp += f's:{len(address)}:"{address}";'
    # schema_descp += f's:15:"addressLocality";s:{len(city)}:"{city}";'
    # schema_descp += f's:13:"addressRegion";s:5:"Texas";'
    # schema_descp += f's:10:"postalCode";s:{len(pin_code)}:"{pin_code}";'
    # schema_descp += f's:14:"addressCountry";s:3:"USA";'
    # schema_descp += '}s:3:"geo";a:3:{s:5:"@type";s:14:"GeoCoordinates";s:8:"latitude";'
    # schema_descp += f's:{len(lat)}:"{lat}";s:9:"longitude";s:{len(long)}:"{long}";'
    # schema_descp += '}s:25:"openingHoursSpecification";a:0:{}'
    # schema_descp += 's:13:"servesCuisine";a:1:{i:0;'
    # schema_descp += f's:{len(cuisine)}:"{cuisine}";'
    # schema_descp += '}s:7:"hasMenu";'
    # schema_descp += f's:{len(website_url)}:"{website_url}";'
    # schema_descp += 's:5:"image";a:2:{s:5:"@type";s:11:"ImageObject";s:3:"url";s:16:"%post_thumbnail%";}}'
    meta_object["rank_math_title"] = rank_title
    meta_object["rank_math_description"] = rank_description
    meta_object["rank_math_focus_keyword"] = focus_keyword
    meta_object["rank_math_schema_Restaurant"] = schema_descp

    meta_id = None
    for index, value in meta_object.items():
        try:
            sql2 = """insert into wp_postmeta (post_id,meta_key,meta_value) values(%s, %s, %s)"""
            cursor.execute(sql2, (last_insert_id, index, value))
            if index == "rank_math_schema_Restaurant":
                meta_id = cursor.lastrowid
        except:
            logger.debug(msg="Error in meta data::")

    if meta_id is not None:
        meta_sql = """insert into wp_postmeta (post_id,meta_key,meta_value) values(%s, %s, %s)"""
        cursor.execute(meta_sql, (last_insert_id, shortcode, meta_id))

    categories = []
    if (
        d.get("category_cuisine_google") is not None
        and d.get("category_cuisine_google", "").lower() != "restaurant"
        and d.get("category_cuisine_google").lower() != "restaurants"
    ):
        cat = (
            d["category_cuisine_google"]
            .replace("restaurant", "")
            .replace("restaurants", "")
            .replace("Restaurant", "")
            .replace("Restaurants", "")
            .strip()
        )
        categories.append(cat)

    categories.append("Restaurants")
    for cat in categories:
        try:  # check if category already exists otherwise create a new and tag
            cat_slug = slugify(cat)
            insert_tag_category(
                cursor, cat, cat_slug, last_insert_id, "job_listing_category"
            )
        except Exception as e:
            logger.debug(msg=f"Error in inserting category - {e}")

    if d.__contains__("amenties") and len(d["amenties"]) > 0:
        for ament in d["amenties"]:
            try:
                ament_slug = slugify(ament)
                insert_tag_category(
                    cursor, ament, ament_slug, last_insert_id, "job_listing_amenity"
                )
            except Exception as e:
                logger.debug(msg=f"Error in inserting amenities - {e}")

    tags = {
        "Food": "food",
        "Home Delivery": "home-delivery",
        "Restaurant": "restaurant",
    }
    for tag, tag_slug in tags.items():
        try:
            insert_tag_category(
                cursor, tag, tag_slug, last_insert_id, "job_listing_tag"
            )
        except:
            logger.exception(msg=f"Error in inserting tags")

    return last_insert_id


def update_meta(cursor, d, post_id):
    meta_object = {}
    meta_object["_lt_address"] = d["address"]
    meta_object["_lt_price_range"] = "inexpensive"
    if "phone_no" in d and not (isinstance(d["phone_no"], float)):
        meta_object["_lt_phone"] = d["phone_no"]
    if "website" in d and not (isinstance(d["website"], float)):
        meta_object["_lt_website"] = d["website"]
    meta_object["_lt_regions"] = 78

    meta_object["_lt_map_latitude"] = str(d["lat"]) if d.__contains__("lat") else ""
    # meta_object['_lt_map_longitude'] = str(d['long']).replace('-', '') if d.__contains__('long') else ''
    meta_object["_lt_map_longitude"] = str(d["long"]) if d.__contains__("long") else ""
    map_object = ""
    if d.__contains__("lat") and d.__contains__("long"):
        lat = str(d["lat"])
        long = str(d["long"])  # .replace('-', '')
        php_serialized = phpserialize.dumps({"lat": lat, "lng": long})
        map_object += php_serialized.decode("utf-8")
    else:
        map_object = "a:0:{}"
    meta_object["_lt_map"] = map_object
    meta_object["_lt_place_booking"] = (
        'a:3:{s:4:"type";s:4:"info";s:9:"affiliate";a:4:{s:4:"link";s:0:"";s:4:"site";s:0:"";s:6:"link_2";s:0:"";s:6:"site_2";s:0:"";}s:6:"banner";a:1:{s:3:"url";s:0:"";}}'
    )

    print(post_id)
    meta_id = None
    for index, value in meta_object.items():
        try:
            sql2 = """update wp_postmeta set meta_value = %s where post_id = %s and meta_key = %s"""
            cursor.execute(sql2, (value, post_id, index))

        except:
            logger.debug(msg="Error in meta data::")


def update_meta_data(cursor, post_id, post_title, post_name, data, business_hours):
    rank_title, rank_description, focus_keyword, schema_descp, short_code = (
        generate_schema(data, post_title, post_name)
    )
    meta_object = {}
    meta_object["rank_math_title"] = rank_title
    meta_object["rank_math_description"] = rank_description
    meta_object["rank_math_focus_keyword"] = focus_keyword
    meta_object["rank_math_schema_Restaurant"] = schema_descp
    if business_hours != "":
        meta_object["_lt_hours_value"] = business_hours
    # map_object = ''
    # map_object += 'a:2:{s:3:"lat";'
    # map_object += f's:10:"{data['lat']}";'
    # map_object += 's:3:"lng";'
    # map_object += f's:10:"{data['long'].replace('-', '')}";'
    # map_object += '}'
    # meta_object['_lt_map'] = map_object
    meta_object["_lt_place_booking"] = (
        'a:3:{s:4:"type";s:4:"info";s:9:"affiliate";a:4:{s:4:"link";s:0:"";s:4:"site";s:0:"";s:6:"link_2";s:0:"";s:6:"site_2";s:0:"";}s:6:"banner";a:1:{s:3:"url";s:0:"";}}'
    )
    meta_object[short_code] = ""

    meta_sql = "select * from wp_postmeta where post_id = '{}'".format(post_id)
    cursor.execute(meta_sql)
    response = cursor.fetchall()
    for index, value in meta_object.items():
        try:
            sql2 = ""
            if response.__contains__(index):
                if "rank_math_shortcode_schema_s" in index:
                    sql_short_code = (
                        """update wp_postmeta where post_id = %s set meta_key = %s"""
                    )
                    cursor.execute(sql_short_code, (post_id, index))
                else:
                    sql2 = """update wp_postmeta where post_id = %s and meta_key = %s set meta_value = %s"""
            else:
                sql2 = """insert into wp_postmeta (post_id,meta_key,meta_value) values(%s, %s, %s)"""
                # cursor.execute(sql2)
            cursor.execute(sql2, (post_id, index, value))
        except:
            logger.exception(msg="Error in updating meta data::")


def generate_schema(d, post_title, post_name):
    city = f"{d.get('city', '')}"
    state = f"{d.get('state', '')}"
    loc_code = f" ({d.get('city', '')}, {d.get('state_postal_abb', '')}) "
    post_title = post_title.replace("'", "")
    post_title_fix = (
        post_title.replace("&", "&amp;").replace("@", "&#64;").replace("#", "&#35;")
    )
    rank_title = (
        post_title_fix + f"{loc_code} Latest Menu - " + current_date.strftime("%B %Y")
    )
    cuisine = (
        d.get("category_cuisine_google", "")
        .replace("restaurant", "")
        .replace("restaurants", "")
        .replace("Restaurant", "")
        .replace("Restaurants", "")
        .strip()
    )
    rank_description = post_title_fix + " | " + d["address"]
    if cuisine != "":
        rank_description = rank_description + " | Cuisine: " + cuisine
    else:
        rank_description = rank_description + " | Cuisine: Restaurant"
    focus_keyword = f"{post_title},{post_title} menu,{post_title} Restaurant Menu,{post_title} menu {city},Best restaurants in {city},{post_title} Menu with prices,{post_title} near me,{post_title} {city},{post_title} kids menu,Top 10 restaurants in {city}"
    if cuisine != "":
        focus_keyword = f"{focus_keyword},{cuisine},{cuisine} near me,{cuisine} near me open now,Best {cuisine} near me"
    else:
        focus_keyword = f"{focus_keyword},Restaurants,Restaurants near me,Restaurants near me open now,Best Restaurants near me"

    random_code = random.choices(string.ascii_lowercase + string.digits, k=13)
    random_code = "".join(random_code)
    shortcode = "rank_math_shortcode_schema_s-" + random_code
    schema_descp = ""

    schema_descp += 'a:12:{s:8:"metadata";a:5:{s:5:"title";s:10:"Restaurant";s:4:"type";s:8:"template";s:9:"shortcode";'
    schema_descp += f's:{len(random_code) + 2}:"s-{random_code}";s:9:"isPrimary";s:1:"1";s:23:"reviewLocationShortcode";s:24:"[rank_math_rich_snippet]";'
    schema_descp += "}"
    schema_descp += 's:5:"@type";s:10:"Restaurant";'
    schema_descp += f's:4:"name";s:{len(rank_title)}:"{rank_title}";'
    schema_descp += (
        f's:11:"description";s:{len(rank_description)}:"{rank_description}";'
    )
    telephone = ""
    if d["phone_no"] != "" and not (isinstance(d["phone_no"], float)):
        telephone = d["phone_no"]

    's:17:"5404 Menchaca Rd ";s:15:"addressLocality";s:6:"Austin";s:13:"addressRegion";s:5:"Texas";s:10:"postalCode";s:5:"78745";s:14:"addressCountry";s:3:"USA";}s:3:"geo";a:3:{s:5:"@type";s:14:"GeoCoordinates";s:8:"latitude";s:10:"30.3460397";s:9:"longitude";s:10:"59.2340242";}s:25:"openingHoursSpecification";a:0:{}s:13:"servesCuisine";a:0:{}s:7:"hasMenu";s:49:"https://top-menus.com/listings/austin-java-menu-2";s:5:"image";a:2:{s:5:"@type";s:11:"ImageObject";s:3:"url";s:16:"%post_thumbnail%";}}'
    schema_descp += f's:9:"telephone";s:{len(telephone)}:"{telephone}";'
    schema_descp += 's:10:"priceRange";s:1:"$";s:7:"address";a:6:{s:5:"@type";s:13:"PostalAddress";s:13:"streetAddress";'
    address = ""
    pin_code = ""
    city = ""
    if d["address"] != "":
        address_arr = d["address"].split(",")
        address = address_arr[0].strip() if len(address_arr) > 0 else ""
        city = address_arr[1].strip() if len(address_arr) > 1 else ""
        pin_code = (
            address_arr[2].replace("TX", "").strip() if len(address_arr) > 3 else ""
        )

    lat = d["lat"] if d.__contains__("lat") else ""
    # long = d['long'].replace('-', '') if d.__contains__('long') else ''
    long = str(d["long"]) if d.__contains__("long") else ""
    's:7:"hasMenu";s:49:"https://top-menus.com/listings/austin-java-menu-2";s:5:"image";a:2:{s:5:"@type";s:11:"ImageObject";s:3:"url";s:16:"%post_thumbnail%";}}'
    website_url = "https://top-menus.com/listings/" + post_name
    schema_descp += f's:{len(address)}:"{address}";'
    schema_descp += f's:15:"addressLocality";s:{len(city)}:"{city}";'
    schema_descp += f's:13:"addressRegion";s:5:"Texas";'
    schema_descp += f's:10:"postalCode";s:{len(pin_code)}:"{pin_code}";'
    schema_descp += f's:14:"addressCountry";s:3:"USA";'
    schema_descp += '}s:3:"geo";a:3:{s:5:"@type";s:14:"GeoCoordinates";s:8:"latitude";'
    schema_descp += f's:{len(lat)}:"{lat}";s:9:"longitude";s:{len(long)}:"{long}";'
    schema_descp += '}s:25:"openingHoursSpecification";a:0:{}'
    schema_descp += 's:13:"servesCuisine";a:1:{i:0;'
    schema_descp += f's:{len(cuisine)}:"{cuisine}";'
    schema_descp += '}s:7:"hasMenu";'
    schema_descp += f's:{len(website_url)}:"{website_url}";'
    schema_descp += 's:5:"image";a:2:{s:5:"@type";s:11:"ImageObject";s:3:"url";s:16:"%post_thumbnail%";}}'

    return rank_title, rank_description, focus_keyword, schema_descp, shortcode


def insert_tag_category(cursor, cat_name, cat_slug, post_id, type):
    cat_sql = """SELECT term_id, name, slug FROM wp_terms where slug = %s"""
    cursor.execute(cat_sql, (cat_slug))
    cat_db = cursor.fetchone()
    if cat_db:
        sql3 = """INSERT INTO wp_term_relationships (object_id,term_taxonomy_id) VALUES (%s, %s)"""
        cursor.execute(sql3, (post_id, cat_db["term_id"]))
    else:
        insert_terms_sql = """
        INSERT INTO wp_terms (name, slug, term_group)
        VALUES (%s, %s, 0)
        """
        cursor.execute(insert_terms_sql, (cat_name, cat_slug))
        term_id = cursor.lastrowid

        # Insert into wp_term_taxonomy
        insert_term_taxonomy_sql = """
        INSERT INTO wp_term_taxonomy (term_id, taxonomy, description, parent, count)
        VALUES (%s, %s, '', 0, 1)
        """
        cursor.execute(insert_term_taxonomy_sql, (term_id, type))

        sql3 = """INSERT INTO wp_term_relationships (object_id,term_taxonomy_id) VALUES (%s, %s)"""
        cursor.execute(sql3, (post_id, term_id))


def string_to_hex(s):
    s = str(s)
    # print(type(s))
    hash_object = hashlib.md5(s.encode())
    hex_code = hash_object.hexdigest()[:6]
    return hex_code


def generate_unique_slug(_id, value):

    unique_id = string_to_hex(_id)
    base_slug = slugify(value)
    unique_slug = f"{base_slug}-{unique_id}"
    counter = 1

    while unique_slug in slugSet:
        unique_slug = f"{base_slug}-{counter}-{unique_id}"
        counter += 1

    slugSet.add(unique_slug)
    return unique_slug


def slugify(text, separator="-", language="en"):
    # Normalize the text to decompose combined characters
    text = unicodedata.normalize("NFKD", text)

    # Convert all dashes/underscores into the separator
    flip = "_" if separator == "-" else "-"
    text = re.sub(r"[" + re.escape(flip) + "]+", separator, text)

    # Replace @ with the word 'at'
    text = text.replace("@", separator + "at" + separator)

    # Replace & with the word 'and'
    text = text.replace("&", separator + "and" + separator)

    # Remove all characters that are not the separator, letters, numbers, or whitespace
    text = re.sub(r"[^" + re.escape(separator) + r"\w\s]+", "", text.lower())

    # Replace all separator characters and whitespace by a single separator
    text = re.sub(r"[" + re.escape(separator) + r"\s]+", separator, text)

    # Strip any leading/trailing separator
    return text.strip(separator)


def convert_to_24h(time_str, colon=False):
    format_str = "%I:%M %p" if colon else "%I %p"
    time_obj = datetime.strptime(time_str, format_str)
    return time_obj.strftime("%H:%M")


def process(rd):

    with concurrent.futures.ThreadPoolExecutor(30) as executor:
        futures = {executor.submit(process_data, r) for r in rd}
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    logger.info(f"--")
    logger.info(f"Started at {datetime.now(tz=pytz.UTC).isoformat()}")
    parser = argparse.ArgumentParser(
        description="Wordpress Top Menus ( Its update works on menu data and will not update category or meta data)"
    )
    parser.add_argument(
        "--name",
        type=str,
        default="",
        help="Enter the restaurant name, separating each with a comma.",
    )
    parser.add_argument("--limit", type=str, default=0, help="Enter the limit")

    args = parser.parse_args()
    # restaurant_names = args.name.split(',')

    restaurant_names = []
    limit = None

    if len(restaurant_names) > 0:
        # data = ds_db.data_publish.find({'name': {"$in" : restaurant_names}}, project )
        data = ds_db.onemenus_ocr.find(
            {"google_id": {"$in": restaurant_names}, "extracted_dishes": {"$ne": []}},
            project,
        )
    else:
        last_cn = datetime.now(tz=pytz.UTC) - timedelta(days=2)
        # last_cn = datetime.now(tz=pytz.UTC) - timedelta(hours=12)
        # last_cn = last_cn.replace(hour=0, minute=0, second=0, microsecond=0)  # Fix to set time to midnight

        data = (
            ds_db.onemenus_ocr.find(
                {
                    "$and": [
                        {
                            "$or": [
                                {
                                    "$and": [
                                        {"topmenus_republished_at": {"$exists": True}},
                                        {
                                            "$expr": {
                                                "$gt": [
                                                    "$updated_on",
                                                    "$topmenus_republished_at",
                                                ]
                                            }
                                        },
                                    ]
                                },
                                {
                                    "$and": [
                                        {"topmenus_republished_at": {"$exists": False}},
                                        {
                                            "$expr": {
                                                "$gt": [
                                                    "$updated_on",
                                                    "$topmenus_published_at",
                                                ]
                                            }
                                        },
                                    ]
                                },
                                {"published": {"$exists": False}},
                            ]
                        },
                        {"extracted_dishes": {"$ne": []}},
                        # {
                        # '$or': [
                        #     { 'created_on': { '$gte': last_cn } },
                        #     { 'updated_on': { '$gte': last_cn } }
                        # ]
                        # }
                    ]
                },
                project,
            )
            .sort([("created_on", -1)])
            .limit(WORDPRESS_LIMIT)
        )

    datas = list(data)
    logger.info(f"Total {len(datas)} found")
    data1 = process_data(datas)
    logger.info(f"--")
