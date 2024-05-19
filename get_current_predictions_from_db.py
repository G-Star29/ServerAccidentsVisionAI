from datetime import datetime
import psycopg2


def get_current_predictions_from_db():
    dbname = 'accidentsvisionai'
    user = 'postgres'
    password = 'Nikita232398'
    host = 'localhost'

    # Подключаемся к базе данных PostgreSQL
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host
    )
    cur = conn.cursor()

    # Получаем текущий час
    current_hour = datetime.now().hour

    # SQL-запрос для извлечения данных с условием по текущему часу
    extract_query = f"""
       SELECT c.col_2, c.col_3, p.prediction_value
       FROM accidentvisionai.predictions_results p
       JOIN accidentvisionai.coords_and_nearby c
       ON p.coords_id = c.col_1
       WHERE EXTRACT(HOUR FROM p.prediction_time) = {current_hour};
       """

    cur.execute(extract_query)
    extracted_data = cur.fetchall()

    # Закрытие соединения
    cur.close()
    conn.close()

    # Преобразование данных в словарь
    extracted_dict = {(row[0], row[1]): row[2] for row in extracted_data}

    return extracted_dict