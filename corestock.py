from dotenv import load_dotenv
import pymysql
import pymysql.cursors
import os
import datetime

## MySQL Connection Info
load_dotenv()
host = os.environ.get("DB_HOST")
database = os.environ.get("DB_NAME")
user = os.environ.get("DB_USER")
password = os.environ.get("DB_PASSWORD")

print(host, user)

# MySQL 연결
connection = pymysql.connect(host=host, user=user, password=password, database=database, cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # 실행할 쿼리들
        queries = [
            "DELETE FROM CoreStock WHERE market_type='KOSPI';",
            "DELETE FROM CoreStock WHERE market_type='KOSDAQ';",
            "INSERT INTO CoreStock SELECT * FROM Stock WHERE market_type='KOSPI' ORDER BY updated_at, market_cap DESC LIMIT 200;",
            "INSERT INTO CoreStock SELECT * FROM Stock WHERE market_type='KOSDAQ' ORDER BY updated_at, market_cap DESC LIMIT 130;"
        ]
        
        # 각 쿼리 실행
        for query in queries:
            cursor.execute(query)
            connection.commit()
            print("Query successful:", query)
        
        
        print("Data Inserted Success")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    connection.close()
