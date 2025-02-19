import pymysql

host = "174.138.54.30"       # e.g., "localhost" or "127.0.0.1"
user = "manish_goyal"
password = "xzmgcLoXx4jYINP6"
database = "wordpress"  # Optional

# host = "5.189.184.167"       # e.g., "localhost" or "127.0.0.1"
# user = "admin_topmenus"
# password = "4NxJfLvMao"
# database = "admin_topmenus"  # Optional

try:
    # Establish connection
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,  # If you just want to connect to the server, you can remove this line
        cursorclass=pymysql.cursors.DictCursor,  # Optional: Returns results as dictionaries
        connect_timeout=60
        # port=3306,  # Default MySQL port
    )

    print(f"✅ Connected to MySQL server successfully! {host}, {user}")

    # # Example query: Retrieve the MySQL version
    # with connection.cursor() as cursor:
    #     cursor.execute("SELECT VERSION()")
    #     version = cursor.fetchone()
    #     print(f"MySQL Server Version: {version['VERSION()']}")

except pymysql.MySQLError as e:
    print(f"❌ Error connecting to MySQL: {e}")
