from celery import Celery
import concurrent.futures
from googlemaps import GoogleMapsScraper
from termcolor import colored
import json
from bson.objectid import ObjectId
from dotenv import load_dotenv

load_dotenv(override=True)
from os import getenv
import traceback
import sys
import os
import re
import publish_recrawling_queue
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from mongo import MongoClient
from constant import *
from logger import ErrorLogger
from config import *
import time
from crawling_status_enum import Status
from copy import deepcopy
from celery.exceptions import MaxRetriesExceededError
from helper.insert_image_mongo import *
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
import re
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne

logger_file_name = "google-map-review-scrapper"
logger = ErrorLogger(log_file_name=logger_file_name, log_to_terminal=True)

app = Celery("consume_publish", broker=getenv("REDIS_URI"))
app.conf.worker_prefetch_multiplier = 1

QUEUE_NAME = "restaurant_reviews"
app.conf.update(
    task_default_queue=QUEUE_NAME,
    task_default_exchange=QUEUE_NAME,
    task_default_routing_key=QUEUE_NAME,
    task_default_exchange_type="direct",
    broker_connection_retry_on_startup=True,
    task_acks_late=True,  # Acknowledging tasks only after completion
    task_reject_on_worker_lost=True,  # Avoid task loss when worker crashes
)


# Function to perform bulk_write and log failures
def perform_bulk_write(batch_operations, restaurant_crawled_reviews_db):
    failed_docs = []  # List to hold the failed operations
    try:
        result = restaurant_crawled_reviews_db.bulk_write(
            batch_operations, ordered=False
        )
        print(
            f"Matched: {result.matched_count}, Modified: {result.modified_count}, Upserts: {result.upserted_count}"
        )

    except BulkWriteError as e:
        # Handle the failure and capture the failed documents
        print(f"BulkWriteError: {e.details}")
        for error in e.details["writeErrors"]:
            failed_docs.append(error["op"])

    # Return the list of failed documents for logging
    return failed_docs


def insert_mongo(operations):
    restaurant_crawled_reviews_db = MongoClient(
        collection=getenv("RESTAURANT_CRAWLED_REVIEWS")
    ).collection
    # Split operations into smaller batches (e.g., 100 operations per batch)
    batch_size = 20
    batches = [
        operations[i : i + batch_size] for i in range(0, len(operations), batch_size)
    ]

    # Collect the failed documents
    all_failed_docs = []

    with concurrent.futures.ThreadPoolExecutor(5) as executor:
        # Submit the batches to the thread pool for parallel execution
        futures = [
            executor.submit(perform_bulk_write, batch, restaurant_crawled_reviews_db)
            for batch in batches
        ]

        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            failed_docs = future.result()  # To catch exceptions if any
            if failed_docs:
                all_failed_docs.append(failed_docs)

    return all_failed_docs


def convert_to_absolute_date(self, relative_time_str, data):
    try:
        # Current time
        now = datetime.now(timezone.utc)

        if relative_time_str == "just now":
            return now

        # Regular expressions to match relative time formats
        time_units = {
            "minute": "minutes",
            "hour": "hours",
            "day": "days",
            "week": "weeks",
            "month": "months",
            "year": "years",
        }

        # Check for common relative time patterns like "2 minutes ago", "1 hour ago"
        for unit in time_units:
            if unit in relative_time_str:
                temp = relative_time_str.split(" ")
                if temp[0] == "a" or temp[0] == "an":
                    temp[0] = "1"
                    relative_time_str = " ".join(temp)

                # Extract the number before the unit (e.g., "2" from "2 minutes ago")
                num = int(re.search(r"\d+", relative_time_str).group())
                # Adjust the current time by subtracting the specified number of units
                return now - relativedelta(**{time_units[unit]: num})
    except Exception as e:
        error_logs = MongoClient(collection=getenv("GOOGLE_MAPS_ERROR_COLLECTION"))
        print("error in getting absolute date", e)
        error_logs.update_errors(
            e,
            {
                "listing_id": data.get("_id"),
                "g_map_id": data["google_id"],
                "url": data.get("map_url"),
                "retry": self.request.retries,
                "origin": "consume_map_url_reviews",
                "metadata": {
                    "functionName": "convert_to_absolute_date",
                    "relative_time_str": relative_time_str,
                    "note": "error while calculating absolute date",
                },
            },
        )


def retry_jobs(self, data):
    backoff = 2**self.request.retries * 360
    try:
        self.retry(countdown=backoff)
    except MaxRetriesExceededError:
        print("Max retries exceeded")
        db_google_entry_url = MongoClient(collection=getenv("ENTRY_URL_COLLECTION"))
        restaurant_crawled_reviews_status = MongoClient(
            collection=getenv("RESTAURANT_CRAWLED_REVIEW_STATUS")
        )
        db_google_entry_url.update_review_status_entry_url(
            "google_id", data["google_id"], Status.FAILED.value
        )
        restaurant_crawled_reviews_status.collection.update_one(
            {"google_id": data["google_id"]},
            {"$set": {"retry": self.request.retries, "status": Status.FAILED.value}},
            upsert=True,
        )


# Global variable to hold the browser session
scraper = None


def init_driver():
    global scraper
    if scraper is None:
        print("Driver initialized.")
        scraper = GoogleMapsScraper()
    return scraper


def shutdown_driver():
    global scraper
    try:
        if scraper:
            scraper.__exit__()  # Ensure any cleanup happens here
            scraper = None  # This properly removes the reference
            print("Scraper closed during worker shutdown.")
    except Exception as e:
        print(f"Error during scraper shutdown: {e}")
        scraper = None  # Ensure global variable is set to None in case of error
        print("Scraper reference cleared due to error.")


@app.task(bind=True, max_retries=3, soft_time_limit=10800, name=QUEUE_NAME)
def consume(self, *args, **kwargs):
    data = self.request.args[0]
    if not isinstance(data, dict):
        data = json.loads(self.request.args[0])
    scraper = None
    scraper = init_driver()
    map_url = data["map_url"]
    time_taken_logs = []
    start_time = datetime.now(timezone.utc)

    try:
        operations = []
        db_google_entry_url = MongoClient(
            collection=getenv("ENTRY_URL_COLLECTION")
        )  # google_entry_url # crawler database
        error_logs = MongoClient(collection=getenv("GOOGLE_MAPS_ERROR_COLLECTION"))
        restaurant_crawled_reviews = MongoClient(
            collection=getenv("RESTAURANT_CRAWLED_REVIEWS")
        )
        restaurant_crawled_reviews_status = MongoClient(
            collection=getenv("RESTAURANT_CRAWLED_REVIEW_STATUS")
        )
        print("Received", data)

        should_retry = False

        if not isinstance(data, dict):
            data = json.loads(data)
        google_maps_data = None

        reviews_status_db = restaurant_crawled_reviews_status.collection.find_one(
            {"google_id": data["google_id"]}
        )

        if (
            reviews_status_db is not None
            and reviews_status_db["status"] == Status.COMPLETED.value
        ):
            return

        if (
            reviews_status_db is not None
            and len(reviews_status_db.get("time_taken_logs", [])) != 0
        ):
            time_taken_logs = reviews_status_db["time_taken_logs"]

        time_taken_logs.append({"start_time": start_time})

        db_google_entry_url.collection.update_one(
            {"google_id": data["google_id"]},
            {"$set": {"review_status": Status.PROCESSING.value}},
            upsert=True,
        )
        restaurant_crawled_reviews_status.collection.update_one(
            {"google_id": data["google_id"]},
            {
                "$set": {
                    "server": SERVER_NAME,
                    "status": Status.PROCESSING.value,
                    "retry": self.request.retries,
                    "map_url": map_url,
                    "time_taken_logs": time_taken_logs,
                }
            },
            upsert=True,
        )
        # scraper = GoogleMapsScraper(logger_file_name=logger_file_name, debug=DEBUG)

        logger.info(msg="Review Scraping Start for url - " + map_url)

        # check if url already scraped
        google_maps_data = list(
            restaurant_crawled_reviews.collection.find({"google_id": data["google_id"]})
        )

        ################## REVIEWS SCRAPING #########################
        review_len = 0
        existing_ids = {review["id_review"]: review for review in google_maps_data}

        reviews_img = []

        total_reviews_count = 0

        reviews_count = REVIEWS_COUNT

        for index, sort in SORTING.items():
            try:
                error = scraper.sort_by(
                    map_url, sort
                )  # 0 : most relevant reviews, 1 : newest reviews
                if total_reviews_count == 0:
                    total_reviews_count = scraper.get_total_reviews(data)
                    reviews_count = total_reviews_count
                    if index == "most_relevant":
                        reviews_count = min(100, total_reviews_count)

                if index == "most_relevant":
                    reviews_count = min(100, total_reviews_count)
                else:
                    reviews_count = total_reviews_count

                n = 0
                sleep_time = 0
                retry_count = 0
                if error == 0:
                    while reviews_count > n:
                        # logging to std out
                        print(colored("[Review " + str(n) + "]", "magenta"))
                        reviews = scraper.get_reviews(n, index, sleep_time)

                        for i, review in enumerate(reviews):
                            if review["id_review"] not in existing_ids:
                                review["review_type"] = [index]
                                review["absolute_date"] = convert_to_absolute_date(
                                    self, review["relative_date"].lower(), data
                                )
                                review["sorting_fields_indexes"] = {index: n + i}

                                existing_ids[review["id_review"]] = (
                                    review  # Add the id_review to the map
                                )
                                for image in review["review_imgs"]:
                                    reviews_img.append(
                                        {
                                            "img_url": image,
                                            "id_review": review["id_review"],
                                        }
                                    )
                            else:
                                existing_ids[review["id_review"]][
                                    "sorting_fields_indexes"
                                ][index] = (n + i)
                                if (
                                    index
                                    not in existing_ids[review["id_review"]][
                                        "review_type"
                                    ]
                                ):
                                    existing_ids[review["id_review"]][
                                        "review_type"
                                    ].append(index)

                        print("existing: ", len(existing_ids))

                        if len(reviews) == 0:
                            if retry_count >= 10:
                                break

                            retry_count = retry_count + 1
                            sleep_time = 1.5
                        else:
                            retry_count = 0
                            sleep_time = 0

                        n += len(reviews)
                        review_len += len(reviews)
                    logger.info(msg=index + " Reviews Scraped - " + map_url)
            except Exception as e:
                logger.debug(msg=f"Error in scraping {index} Review - {map_url}")
                error_logs.update_errors(
                    e,
                    {
                        "listing_id": data.get("_id"),
                        "g_map_id": data["google_id"],
                        "url": data.get("map_url"),
                        "html": scraper.driver.page_source if scraper else "",
                        "retry": self.request.retries,
                        "origin": "consume_map_url_reviews",
                        "metadata": {
                            "index": index,
                            "note": "error while fetching review for the given index",
                        },
                    },
                )
                should_retry = True

        if len(reviews_img) != 0:
            upsert_images_to_mongo({"reviews": [reviews_img]}, data["google_id"])

        # get total reviews count
        # total_reviews_count = scraper.get_total_reviews(data)

        if len(existing_ids) != 0 and (len(existing_ids) / total_reviews_count) < 0.7:
            should_retry = True
            print("reviews crawled is less than the threshold")
            try:
                raise Exception(
                    f"reviews crawled is less than the threshold, reviews_crawled is {len(existing_ids)} out of {total_reviews_count}"
                )
            except Exception as e:
                error_logs.update_errors(
                    e,
                    {
                        "listing_id": data.get("_id"),
                        "g_map_id": data["google_id"],
                        "url": data.get("map_url"),
                        "html": scraper.driver.page_source if scraper else "",
                        "retry": self.request.retries,
                        "origin": "consume_map_url_reviews",
                        "metadata": {
                            "note": "reviews crawled is less than the threshold",
                            "total_reviews_count": total_reviews_count,
                            "reviews_crawled": len(existing_ids),
                        },
                    },
                )

        existing_ids_list = list(existing_ids.values())
        operations = [
            UpdateOne(
                {"google_id": data["google_id"], "id_review": doc["id_review"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in existing_ids_list
        ]

        failed_docs = insert_mongo(operations)

        if len(failed_docs) != 0:
            try:
                raise Exception(f"error while inserting reviews in mongoDB")
            except Exception as e:
                error_logs.update_errors(
                    e,
                    {
                        "listing_id": data.get("_id"),
                        "g_map_id": data["google_id"],
                        "url": data.get("map_url"),
                        "retry": self.request.retries,
                        "origin": "consume_map_url_reviews",
                        "metadata": {
                            "note": "error while inserting reviews in mongoDB",
                            "failed_docs": failed_docs,
                        },
                    },
                )
            print("failed_docs")
            print(failed_docs)

        end_time = datetime.now(timezone.utc)
        time_taken_logs[len(time_taken_logs) - 1] = {
            "start_time": start_time,
            "end_time": end_time,
            "time_taken": (end_time - start_time).total_seconds(),
            "status": "success",
        }

        restaurant_crawled_reviews_status.collection.update_one(
            {"google_id": data["google_id"]},
            {
                "$set": {
                    "total_reviews_count": total_reviews_count,
                    "retry": self.request.retries,
                    "status": (
                        Status.RETRY.value
                        if should_retry == True
                        else Status.COMPLETED.value
                    ),
                    "reviews_crawled": len(existing_ids),
                    "time_taken_logs": time_taken_logs,
                }
            },
            upsert=True,
        )
        db_google_entry_url.update_review_status_entry_url(
            "google_id", data["google_id"], Status.COMPLETED.value
        )

        # scraper.__exit__()
        if should_retry == True:
            retry_jobs(self, data)
            print("backoff")
        # else:
        #     error_logs.collection.find_one_and_delete({"google_id": data['google_id']})
    except Exception as e:
        shutdown_driver()
        # if(scraper):
        #     scraper.__exit__()
        logger.exception(msg="Error on google Maps review crawling - " + map_url)
        error_logs.update_errors(
            e,
            {
                "listing_id": data.get("_id"),
                "g_map_id": data["google_id"],
                "url": data.get("map_url"),
                # "html": scraper.driver.page_source if scraper else "",
                "retry": self.request.retries,
                "origin": "consume_map_url_reviews",
                "metadata": {
                    "note": "error while scrapping the reviews of the given url",
                },
            },
        )

        if data:
            end_time = datetime.now(timezone.utc)
            time_taken_logs[len(time_taken_logs) - 1] = {
                "start_time": start_time,
                "end_time": end_time,
                "time_taken": (end_time - start_time).total_seconds(),
                "status": "failed",
            }

            db_google_entry_url.update_review_status_entry_url(
                "google_id", data["google_id"], Status.RETRY.value
            )
            restaurant_crawled_reviews_status.collection.update_one(
                {"google_id": data["google_id"]},
                {
                    "$set": {
                        "retry": self.request.retries,
                        "status": Status.RETRY.value,
                        "time_taken_logs": time_taken_logs,
                    }
                },
                upsert=True,
            )

        retry_jobs(self, data)
