from fastapi import FastAPI, HTTPException
from datetime import datetime
import psycopg2
from psycopg2 import OperationalError


def check_records_for_current_date_and_hour():
    try:
        dbname = 'accidentsvisionai'
        user = 'postgres'
        password = 'Nikita232398'
        host = 'localhost'

        # Подключение к базе данных PostgreSQL
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host
        )
        cur = conn.cursor()

        # Получение текущей даты и часа
        now = datetime.now()
        current_date = now.date()
        current_hour = now.hour

        # SQL-запрос для проверки наличия записей
        check_query = """
        SELECT COUNT(*)
        FROM accidentvisionai.predictions_results
        WHERE prediction_date = %s AND EXTRACT(HOUR FROM prediction_time) = %s;
        """

        cur.execute(check_query, (current_date, current_hour))
        result = cur.fetchone()[0]

        # Закрытие соединения
        cur.close()
        conn.close()

        return result > 0

    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return False