import os.path
import time
import psycopg2
import datetime

from googleapiclient.discovery import build
from google.oauth2 import service_account
from pycbrf.toolbox import ExchangeRates

from config import *

rates = ExchangeRates(datetime.date.today())

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SERVICE_ACCOUNT_FILE = os.path.join(BASE_DIR, 'credentials.json')

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1E4H3iVGEZLlGBKrh3nTkkVCTw81OnsvGuRMNx3XL5Vs'
SAMPLE_RANGE_NAME = 'Sheet1'


def main():
    service = build('sheets', 'v4', credentials=credentials)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()

    data_from_sheet = result.get('values', [])

    try:
        # connect to exist db
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            port=port
        )

        connection.autocommit = True

        # cursor for performing db operations

        # create a new table

        with connection.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE IF NOT EXISTS info(
                    id int,
                    order_number int NOT NULL,
                    usd int,
                    supply_time date,
                    in_rub float)"""
            )

            print("Table created succesfully!")

        data_now = []
        data_sec = []

        for data in data_from_sheet[1:]:
            cost_in_rub = round(float(data[2]) * float(rates['R01235'].value), 2)
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO info (id, order_number, usd, supply_time, in_rub) 
                    VALUES (%s, %s, %s, %s, %s);""", (data[0], data[1], data[2], data[3], cost_in_rub)
                )

        data_now.append(data)

        # condition if sheets updates

        while True:

            service = build('sheets', 'v4', credentials=credentials)

            # Call the Sheets API
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                        range=SAMPLE_RANGE_NAME).execute()

            data_from_now = result.get('values', [])

            time.sleep(2)

            for data in data_from_now[1:]:
                data_sec.append(data)

            if data_now != data_sec:

                with connection.cursor() as cursor:
                    cursor.execute(
                        """DELETE FROM info""",
                    )

                for data in data_from_now[1:]:
                    cost_in_rub = round(float(data[2]) * float(rates['R01235'].value), 2)
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """INSERT INTO info (id, order_number, usd, supply_time, in_rub) 
                            VALUES (%s, %s, %s, %s, %s);""", (data[0], data[1], data[2], data[3], cost_in_rub)
                        )

                    data_now.append(data)

    except Exception as _ex:
        print("ERROR while working with PostgreSQL", _ex)
        main()
    finally:
        if connection:
            connection.close()
            print("PostgreSQL conncetion closed")
            main()


if __name__ == '__main__':
    main()