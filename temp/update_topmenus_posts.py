import pymongo
import pymysql

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "your_mongo_db"
MONGO_COLLECTION = "your_mongo_collection"

# MySQL Configuration
MYSQL_HOST = "localhost"
MYSQL_USER = "your_mysql_user"
MYSQL_PASSWORD = "your_mysql_password"
MYSQL_DB = "your_mysql_db"

# Connect to MongoDB
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]
mongo_collection = mongo_db[MONGO_COLLECTION]

# Connect to MySQL
mysql_conn = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    cursorclass=pymysql.cursors.DictCursor
)

try:
    with mysql_conn.cursor() as cursor:
        for doc in mongo_collection.find({}, {"sql_id": 1, "lat": 1, "long": 1, "post_name": 1}):
            sql_id = doc.get("sql_id")
            mongo_lat = doc.get("lat")
            mongo_long = doc.get("long")
            mongo_post_name = doc.get("post_name")

            if not sql_id:
                continue  # Skip documents without sql_id

            # Fetch lat, long from postmeta table
            cursor.execute("SELECT meta_value FROM postmeta WHERE post_id = %s AND meta_key = 'lat'", (sql_id,))
            result_lat = cursor.fetchone()
            sql_lat = float(result_lat['meta_value']) if result_lat else None

            cursor.execute("SELECT meta_value FROM postmeta WHERE post_id = %s AND meta_key = 'long'", (sql_id,))
            result_long = cursor.fetchone()
            sql_long = float(result_long['meta_value']) if result_long else None

            # Update lat if different
            if mongo_lat and sql_lat is not None and mongo_lat != sql_lat:
                cursor.execute(
                    "UPDATE postmeta SET meta_value = %s WHERE post_id = %s AND meta_key = 'lat'",
                    (mongo_lat, sql_id)
                )

            # Update long if different
            if mongo_long and sql_long is not None and mongo_long != sql_long:
                cursor.execute(
                    "UPDATE postmeta SET meta_value = %s WHERE post_id = %s AND meta_key = 'long'",
                    (mongo_long, sql_id)
                )

            # Fetch post_name from posts table
            cursor.execute("SELECT post_name FROM posts WHERE ID = %s", (sql_id,))
            result_post = cursor.fetchone()
            sql_post_name = result_post['post_name'] if result_post else None

            # Update post_name if different
            if mongo_post_name and sql_post_name and mongo_post_name != sql_post_name:
                cursor.execute(
                    "UPDATE posts SET post_name = %s WHERE ID = %s",
                    (mongo_post_name, sql_id)
                )

        mysql_conn.commit()

finally:
    cursor.close()
    mysql_conn.close()
    mongo_client.close()

print("Update process completed.")
