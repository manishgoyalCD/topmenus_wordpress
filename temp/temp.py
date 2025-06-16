import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, select
from urllib.parse import quote_plus
import requests
import os
import time

category_payload = [
    {"id": "0", "text": "All Categories"},
    {"id": "6018", "text": "Books"},
    {"id": "6000", "text": "Business"},
    {"id": "6026", "text": "Developer Tools"},
    {"id": "6017", "text": "Education"},
    {"id": "6016", "text": "Entertainment"},
    {"id": "6015", "text": "Finance"},
    {"id": "6023", "text": "Food & Drink"},
    {
        "id": "6014",
        "text": "GamesGames / ActionGames / AdventureGames / BoardGames / CardGames / CasinoGames / CasualGames / FamilyGames / MusicGames / PuzzleGames / RacingGames / Role PlayingGames / SimulationGames / SportsGames / StrategyGames / TriviaGames / Word",
    },
    {"id": "7001", "text": "Games / Action"},
    {"id": "7002", "text": "Games / Adventure"},
    {"id": "7004", "text": "Games / Board"},
    {"id": "7005", "text": "Games / Card"},
    {"id": "7006", "text": "Games / Casino"},
    {"id": "7003", "text": "Games / Casual"},
    {"id": "7009", "text": "Games / Family"},
    {"id": "7011", "text": "Games / Music"},
    {"id": "7012", "text": "Games / Puzzle"},
    {"id": "7013", "text": "Games / Racing"},
    {"id": "7014", "text": "Games / Role Playing"},
    {"id": "7015", "text": "Games / Simulation"},
    {"id": "7016", "text": "Games / Sports"},
    {"id": "7017", "text": "Games / Strategy"},
    {"id": "7018", "text": "Games / Trivia"},
    {"id": "7019", "text": "Games / Word"},
    {"id": "6027", "text": "Graphics & Design"},
    {"id": "6013", "text": "Health & Fitness"},
    {"id": "9007", "text": "KidsKids / Ages 5 & UnderKids / Ages 6-8Kids / Ages 9-11"},
    {"id": "10000", "text": "Kids / Ages 5 & Under"},
    {"id": "10001", "text": "Kids / Ages 6-8"},
    {"id": "10002", "text": "Kids / Ages 9-11"},
    {"id": "6012", "text": "Lifestyle"},
    {"id": "6020", "text": "Medical"},
    {"id": "6011", "text": "Music"},
    {"id": "6010", "text": "Navigation"},
    {"id": "6009", "text": "News"},
    {"id": "6008", "text": "Photo & Video"},
    {"id": "6007", "text": "Productivity"},
    {"id": "6006", "text": "Reference"},
    {"id": "6024", "text": "Shopping"},
    {"id": "6005", "text": "Social Networking"},
    {"id": "6004", "text": "Sports"},
    {"id": "6003", "text": "Travel"},
    {"id": "6002", "text": "Utilities"},
    {"id": "6001", "text": "Weather"},
]

regions = [
    "US",
    "AU",
    "CA",
    "CN",
    "FR",
    "DE",
    "GB",
    "IT",
    "JP",
    "RU",
    "KR",
    "DZ",
    "AO",
    "AR",
    "AT",
    "AZ",
    "BH",
    "BB",
    "BY",
    "BE",
    "BM",
    "BO",
    "BR",
    "BG",
    "KH",
    "CL",
    "CO",
    "CR",
    "HR",
    "CY",
    "CZ",
    "DK",
    "DO",
    "EC",
    "EG",
    "SV",
    "EE",
    "FI",
    "GE",
    "GH",
    "GR",
    "GT",
    "HK",
    "HU",
    "IN",
    "ID",
    "IE",
    "IL",
    "KZ",
    "KE",
    "KW",
    "LV",
    "LB",
    "LT",
    "LU",
    "MO",
    "MG",
    "MY",
    "MT",
    "MX",
    "NL",
    "NZ",
    "NI",
    "NG",
    "NO",
    "OM",
    "PK",
    "PA",
    "PY",
    "PE",
    "PH",
    "PL",
    "PT",
    "QA",
    "RO",
    "SA",
    "RS",
    "SG",
    "SK",
    "SI",
    "ZA",
    "ES",
    "LK",
    "SE",
    "CH",
    "TW",
    "TH",
    "TN",
    "TR",
    "UA",
    "AE",
    "UY",
    "UZ",
    "VE",
    "VN",
]


# Main usage
host = "91.203.132.40"
user = "developer_user"
password = "leLtfDeeEVcehLCoiPk@ewRUu%gUnms%EyrYvqxZ"
database = "collegedunia_test"

# Database connection details
db_config = {
    "username": user,
    "password": password,
    "host": "91.203.132.40",
    "port": 3306,
    "database": database,
}

# URL encode the username and password
username = quote_plus(db_config["username"])
password = quote_plus(db_config["password"])
# Reflect the table
try:
    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    connection = engine.connect()
    print("Connection successful!")
    connection.close()
except Exception as e:
    print("Connection failed:", e)
    quit()

metadata = MetaData()
downloaded_data_table = Table("sensor_tower_daily", metadata, autoload_with=engine)


def is_already_downloaded(category_id, start_date, device, region):
    """
    Check if data for the given category_id, start_date, device, and region has already been downloaded.
    """
    query = select(downloaded_data_table).where(
        (downloaded_data_table.c.category_id == category_id)
        & (
            downloaded_data_table.c.date == start_date
        )  # Use the correct column name for the date
        & (
            downloaded_data_table.c.divice == device
        )  # Use the correct column name for the device
        & (downloaded_data_table.c.region == region)
    )
    with engine.connect() as connection:
        result = connection.execute(query).fetchone()
        # print(result, '././././././')
        return result is not None


def insert_data(df, category_id, start_date, end_date, devices, regions):
    column_mapping = {
        "Unified Name": "name",
        "Unified ID": "unified_id",
        "Unified Publisher Name": "unified_publisher_name",
        "Unified Publisher ID": "unified_publisher_id",
        "Date": "date",
        "Platform": "platform",
        "Category": "category",
        "Downloads (Absolute)": "downloads_absolute",
        "Downloads (Growth)": "downloads_growth",
        "Downloads (Growth %)": "downloads_growth_percent",
        "Revenue (Absolute, $)": "revenue_absolute",
        "Revenue (Growth, $)": "revenue_growth",
        "Revenue (Growth %)": "revenue_growth_percent",
        "Average DAU (Absolute)": "average_dau_absolute",
        "Average DAU (Growth)": "average_dau_growth",
        "Average DAU (Growth %)": "average_dau_growth_percent",
        "Release Date (WW)": "release_date_ww",
        "Earliest Release Date": "earliest_release_date",
        "Publisher Country": "publisher_country",
        "Most Popular Country by Downloads": "most_popular_country_by_downloads",
        "Organic Downloads % (Last Q, WW)": "organic_downloads_percent_last_q_ww",
        "Paid Downloads % (Last Q, WW)": "paid_downloads_percent_last_q_ww",
        "All Time Downloads (WW)": "all_time_downloads_ww",
    }

    # rename the columns to match the column names in the API response
    df.rename(columns=column_mapping, inplace=True)
    # select specific columns
    df["divice"] = devices
    df["region"] = regions
    df["category_id"] = int(category_id)
    df_new = df
    df = df[
        [
            "name",
            "divice",
            "region",
            "category_id",
            "date",
            "category",
            "release_date_ww",
            "earliest_release_date",
            "publisher_country",
            "all_time_downloads_ww",
        ]
    ]
    print(df)
    df_new = df_new[
        [
            "name",
            "divice",
            "region",
            "category_id",
            "unified_publisher_name",
            "date",
            "category",
            "downloads_absolute",
            "downloads_growth",
            "downloads_growth_percent",
            "revenue_absolute",
            "revenue_growth",
            "revenue_growth_percent",
            "average_dau_absolute",
            "average_dau_growth",
            "average_dau_growth_percent",
            "most_popular_country_by_downloads",
            "organic_downloads_percent_last_q_ww",
            "paid_downloads_percent_last_q_ww",
        ]
    ]
    print(df_new)
    # ALTER TABLE `sensor_tower` ADD COLUMN divice varchar(255) default null

    # Insert data into the table
    try:
        df.to_sql("sensor_tower_overall", con=engine, if_exists="append", index=False)
        print("Data inserted successfully.")
    except Exception as e:
        print("Error while inserting data:", e)

    # Insert data into the table
    try:
        df_new.to_sql("sensor_tower_daily", con=engine, if_exists="append", index=False)
        print("Data inserted successfully.")
    except Exception as e:
        print("Error while inserting data:", e)


def download_apps_data(
    category_id=0,
    start_date="2025-03-04",
    end_date="2025-03-04",
    devices=["iphone", "ipad", "android"],
    regions=["US"],
):
    # API endpoint
    url = "https://app.sensortower.com/api/unified/top_apps/with_facets.csv"

    # Headers for the request
    headers = {
        "accept": "*/*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type": "application/json",
        "origin": "https://app.sensortower.com",
        "referer": "https://app.sensortower.com/",
        "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "x-csrf-token": "djdZhXC2q9J2z6psaTzimthHu078wdi/N0YUXYqJpAmvdmbyaKbPjqvHpR1Wkk9F7TFYTKci0PxFny1V3N1e3g==",
    }

    # Cookies for the request
    cookies = {
        "locale": "en",
        "osano_consentmanager_uuid": "239d131e-eb93-41a2-93e2-33d5f44257d9",
        "osano_consentmanager": "IdU4AwPAomNQujOIFMObq2nvsWzv7atx0CFpglbAZGudVbusQ-Mp3s_hynkULvORiqrtsUs5Um-jy2BWvtNg8ESn9JERsSOol7PzXIKg3lh2CffClLKkUWOfkB9sllGnjQ05qGSvtUzxq8UjXOp-2X-CFf0mxJH95MokXMoaeAsYad3PKuOW2ATl266NV-7cCxHL14KX6IlmodNa9ktevdG7IsJdkcKxKXh1losstLW7cTCsY7oS46_xdNKeDAsN82lOoecb647Ipc92Txcu7dFEhbyHo1HFl58Y32Gne9MaLdN74a6qwLzx26aQxpuoEVQv7cmYmEw=",
        "device_id": "ddd75a8d-1b0d-42eb-b92b-cbb80cab6860",
        "sensor_tower_session": "51bfaaf3735d7549361e176b0b6a4e40",
        "_ga": "GA1.1.229504196.1742547164",
        # "_gcl_au": "1.1.1903697950.1742547838",
        # "_mkto_trk": "id:351-RWH-315&token:_mch-sensortower.com-23c2cc20413da2cde5d11786bbc2ad45",
        # "__adroll_fpc": "5057ca278e8938f95d4fbee0cf0d38da-1742547841128",
        # "_zitok": "11cdadede2826239960e1742547842",
        # "_ga_7P9W2ZETPG": "GS1.1.1742547839.1.1.1742549549.60.0.0",
        # "__ar_v4": "KBVXVTIQHNCTJB23HUAR5J%3A20250320%3A2%7CG766IZETCNGLZN7VJ7M6MZ%3A20250320%3A2%7C7OF5EADQIBGZDM5P2PJSAH%3A20250320%3A2",
        "NPS_028709fd_last_seen": "1742550412624",
        "NPS_028709fd_throttle": "1742933363629",
        "_ga_FDNER2EVFL": "GS1.1.1742890150.10.1.1742890169.0.0.0",
        "AMP_6edb64137a": "JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjI0YzNmZmY4Ny1kZTc2LTQ0ODgtOGVjNy01NjZlZDQ2MThjZGElMjIlMkMlMjJ1c2VySWQlMjIlM0ElMjJhbmt1c2gucmF3YXQlNDBjb2xsZWdlZHVuaWEuY29tJTIyJTJDJTIyc2Vzc2lvbklkJTIyJTNBMTc0Mjg4NjI1OTk4NCUyQyUyMm9wdE91dCUyMiUzQWZhbHNlJTJDJTIybGFzdEV2ZW50VGltZSUyMiUzQTE3NDI4OTEzNzgyMTklMkMlMjJsYXN0RXZlbnRJZCUyMiUzQTM0NiUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMCU3RA==",
    }

    # Payload (JSON body)
    payload = {
        "filters": {
            "category": category_id,
            "comparison_attribute": "absolute",
            "start_date": start_date,
            "time_range": "day",
            "end_date": end_date,
            "measure": "downloads",
            "devices": devices,
            "regions": regions,
        },
        "facets": [
            {"measure": "downloads", "type": "absolute"},
            {"type": "custom", "name": "Release Date (WW)"},
            {"type": "custom", "name": "Earliest Release Date"},
            {"type": "custom", "name": "Publisher Country"},
            {"measure": "dau", "type": "absolute"},
            {"type": "custom", "name": "Most Popular Country by Downloads"},
            {"measure": "revenue", "type": "absolute"},
            {"type": "custom", "name": "Organic Downloads % (Last Q, WW)"},
            {"type": "custom", "name": "Paid Downloads % (Last Q, WW)"},
            {"type": "custom", "name": "All Time Downloads (WW)"},
        ],
        "pagination": {"limit": 10000, "offset": 0},
        "use_preview_data": False,
    }

    # Make the request
    response = requests.post(url, headers=headers, cookies=cookies, json=payload)

    # Save the response as a CSV file
    if response.status_code == 200:
        file_path = "top_apps_custom_new_use_pr.csv"
        with open(file_path, "wb") as file:
            file.write(response.content)

        df = pd.read_csv(
            file_path, encoding="utf-16", sep="\t"
        )  # Assuming tab-separated values based on the content
        # print(df.head())
        insert_data(df, category_id, start_date, end_date, devices[0], regions[0])
        os.remove(file_path)
        print("CSV file downloaded successfully as 'top_apps_custom_new_use.csv'.")
    else:
        print(f"Failed to download data. HTTP Status Code: {response.status_code}")
        print(f"Response: {response.text}")
    time.sleep(4)


import sys
from datetime import datetime

if __name__ == "__main__":
    start_date_input = None
    end_date_input = None
    if len(sys.argv) > 1:
        start_date_input = sys.argv[1]
    if len(sys.argv) > 2:
        end_date_input = sys.argv[2]
    # category_id = "10000"  # Replace with the desired category ID
    start_date = "2025-03-09"  # Replace with the desired start date
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    start_date = current_date.strftime("%Y-%m-%d")

    if start_date_input:
        start_date = start_date_input
    end_date = start_date
    if end_date_input:
        end_date = end_date_input
    devices = ["android", "iphone", "ipad"]  # Replace with the desired devices
    # regions = [
    #     "US",
    #     "GB",
    #     "CA",
    #     "AU",
    #     "DE",
    #     "FR",
    #     "IT",
    #     "JP",
    #     "KR",
    #     "MX",
    #     "NL",
    #     "PL",
    #     "RU",
    #     "TR",
    #     "BR",
    #     "IN",
    #     "CN",
    #     "ES",
    #     "SE",
    #     "FI",
    #     "NO",
    #     "DK",
    #     "HK",
    #     "SG",
    #     "TW",
    #     "TH",
    #     "VN",
    #     "ID",
    #     "PH",
    #     "PT",
    #     "QA",
    #     "RO",
    #     "SA",
    #     "RS",
    #     "SK",
    #     "SI",
    #     "ZA",
    #     "LK",
    #     "CH",
    #     "TN",
    #     "UA",
    #     "AE",
    #     "UY",
    #     "UZ",
    #     "VE",
    #     "VN",
    #     # "PL",
    #     # "SG",
    #     # "ES",
    #     # "SE",
    #     # "TW",
    #     # "TH",
    #     # "TR",
    # ]  # Replace with the desired regions

    for device in devices:
        for region in regions:
            for category_id in category_payload:
                cat_id = category_id.get("id")
                res = is_already_downloaded(cat_id, start_date, device, region)
                print(res, "././/./.../././//////")
                if res:
                    print(
                        f"Data for category {cat_id}, device {device}, region {region} already downloaded. Skipping..."
                    )
                    continue
                else:
                    start_time = time.time()
                    print(
                        f"Downloading data for category {cat_id}, date {start_date} end date {end_date} , device {device}, region {region}..."
                    )
                    download_apps_data(
                        category_id=cat_id,
                        start_date=start_date,
                        end_date=end_date,
                        devices=[device],
                        regions=[region],
                    )
                    end_time = time.time()
                    elapsed_time = end_time - start_time
                    print(
                        f"Downloaded data for category {cat_id}, date {start_date} , device {device}, region {region} in {elapsed_time:.2f} seconds."
                    )
    # download_apps_data(category_id, start_date, devices, regions)

# Avg DAU (Date wise)
# Downlaods (Date wise)
