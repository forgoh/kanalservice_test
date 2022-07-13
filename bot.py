import datetime
import time

import psycopg2
import telebot
from config import *

TOKEN = '5546779719:AAHFoEo1ONwsfJcPBDPxVRetU_-DdkFrAqQ'

bot = telebot.TeleBot(TOKEN)

channel = "@kanalservice_test"


def send_info():
    while True:
        try:
            connection = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=db_name,
                port=port
            )

            connection.autocommit = True

            # cursor for performing db operations
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM info;"
                )


                info_records = cursor.fetchall()

                for row in info_records:
                    print(row[0])
                    print(row[1])
                    print(row[2])
                    print(row[3])
                    print(datetime.date.today())
                    print(datetime.date.today() < row[3])
                    print(row[4])
                    if (datetime.date.today() < row[3]) == False:
                        bot.send_message(channel, '❌ ПРОСРОЧЕН Заказ №: ' + str(row[1]) + '\n\n'
                                         + 'Срок поставки: ' + str(row[3]) + '\n\n'
                                         + 'Текущая дата: ' + str(datetime.date.today())
                                         )
                    time.sleep(3)


        except Exception as _ex:
            print("ERROR while working with PostgreSQL", _ex)
        finally:
            if connection:
                connection.close()
                print("PostgreSQL conncetion closed")



