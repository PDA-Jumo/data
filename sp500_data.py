from dotenv import load_dotenv
import FinanceDataReader as fdr
import pymysql
import pymysql.cursors
import os
import datetime

## Stock list
STOCK_MARKET = "S&P500"
BATCH_SIZE = 100

## MySQL Connection Info
load_dotenv()
host = os.environ.get("DB_HOST")
database = os.environ.get("DB_NAME")
user = os.environ.get("DB_USER")
password = os.environ.get("DB_PASSWORD")

## MySQL 연결
connection = pymysql.connect(host=host, user=user, password=password, database=database, cursorclass=pymysql.cursors.DictCursor)

df_SP500 = fdr.StockListing('S&P500')
print(df_SP500.head())

try:
    with connection.cursor() as cursor:
        query = """INSERT INTO Stock (stock_code, stock_name, market_location, market_type, market_cap,updated_at) 
                   VALUES(%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                   market_cap = VALUES(market_cap),
                   updated_at = VALUES(updated_at)"""

        for start_row in range(0, len(df_SP500), BATCH_SIZE):
            batch_data = df_SP500.iloc[start_row:start_row + BATCH_SIZE]

            # 데이터 준비
            data_to_insert = []
            for idx, row in batch_data.iterrows():
                stock_code = row['Symbol']  
                stock_name = row['Name']
                market_cap = 0
                market_location = 1  # 해외주식
                market_type = STOCK_MARKET
                updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                data_to_insert.append((stock_code, stock_name, market_location, market_type, market_cap, updated_at))
            
            # executemany를 사용하여 데이터 삽입
            cursor.executemany(query, data_to_insert)
            connection.commit()
            print("Data Inserted Success")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    connection.close()
