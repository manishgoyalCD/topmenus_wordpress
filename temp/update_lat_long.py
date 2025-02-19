import pymongo
import pymysql
import phpserialize


# MongoDB Configuration
MONGO_URI = "mongodb://dev:ZoP5uhZhgTsR8Fh@38.242.158.36:27017/"
MONGO_DB = "topmenus_product"
MONGO_COLLECTION = "ocr"


# Connect to MongoDB
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]
mongo_collection = mongo_db[MONGO_COLLECTION]

# Connect to MySQL
mysql_conn = pymysql.connect(
    host="174.138.54.30",
    user="manish_goyal",
    password="xzmgcLoXx4jYINP6",
    database="wordpress",
    cursorclass=pymysql.cursors.DictCursor,
)


def get_map_obj(new_lat, new_long):
    """Generate a serialized PHP object for _lt_map field."""
    if new_lat and new_long:
        return phpserialize.dumps({"lat": new_lat, "lng": new_long}).decode("utf-8")
    return "a:0:{}"  # Default empty serialized object


try:
    with mysql_conn.cursor() as cursor:
        for doc in mongo_collection.find(
            {"sql_id": {"$gt": 0}}, {"sql_id": 1, "lat": 1, "long": 1}
        ):
            sql_id = doc["sql_id"]
            new_lat, new_long = doc.get("lat"), doc.get("long")

            # Fetch all relevant meta_values in one query
            cursor.execute(
                """
                SELECT meta_key, meta_value FROM wp_postmeta 
                WHERE post_id = %s AND meta_key IN ('_lt_map_latitude', '_lt_map_longitude', '_lt_map')
            """,
                (sql_id,),
            )
            results = {row["meta_key"]: row["meta_value"] for row in cursor.fetchall()}

            current_lat = float(results.get("_lt_map_latitude") or 0)
            current_long = float(results.get("_lt_map_longitude") or 0)
            update_queries = []

            # Update latitude if different
            if new_lat is not None and new_lat != current_lat:
                update_queries.append(("_lt_map_latitude", new_lat))

            # Update longitude if different
            if new_long is not None and new_long != current_long:
                update_queries.append(("_lt_map_longitude", new_long))

            # Update _lt_map if lat/long changed
            if update_queries:
                new_lt_map = get_map_obj(new_lat, new_long)
                update_queries.append(("_lt_map", new_lt_map))
                print("sql_id: ", sql_id)
                # print("current: ", current_lat, current_long)
                # print("New: ", new_lat, new_long)

            # Perform batch update
            if update_queries:
                cursor.executemany(
                    "UPDATE wp_postmeta SET meta_value = %s WHERE post_id = %s AND meta_key = %s",
                    [(value, sql_id, key) for key, value in update_queries],
                )

        mysql_conn.commit()  # Commit updates

finally:
    cursor.close()
    mysql_conn.close()
    mongo_client.close()

print("Update process completed.")
