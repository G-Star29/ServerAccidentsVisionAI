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

    # SQL-запрос для проверки наличия данных за текущий час
    check_query = f"""
        SELECT 1
        FROM accidentvisionai.predictions_results
        WHERE EXTRACT(HOUR FROM prediction_time) = {current_hour}
        LIMIT 1;
    """
    cur.execute(check_query)
    data_exists = cur.fetchone() is not None

    if not data_exists:
        print('[DATA] ВНИМАНИЕ! Данных на текущий час нет, отправляются последние доступные данные')
        # Если данных за текущий час нет, найти последний доступный час
        last_hour_query = """
            SELECT EXTRACT(HOUR FROM prediction_time) AS last_hour
            FROM accidentvisionai.predictions_results
            ORDER BY prediction_time DESC
            LIMIT 1;
        """
        cur.execute(last_hour_query)
        last_hour = int(cur.fetchone()[0])

        # Извлечь данные за последний доступный час
        extract_query = f"""
           SELECT c.col_2, c.col_3, p.prediction_value
           FROM accidentvisionai.predictions_results p
           JOIN accidentvisionai.coords_and_nearby c
           ON p.coords_id = c.col_1
           WHERE EXTRACT(HOUR FROM p.prediction_time) = {last_hour};
           """
    else:
        print('[DATA] Отправляются данные за последний час')
        # Извлечь данные за текущий час
        extract_query = f"""
           SELECT c.col_2, c.col_3, p.prediction_value
           FROM accidentvisionai.predictions_results p
           JOIN accidentvisionai.coords_and_nearby c
           ON p.coords_id = c.col_1
           WHERE EXTRACT(HOUR FROM p.prediction_time) = {current_hour};
           """

    cur.execute(extract_query)
    extracted_data = cur.fetchall()

    cur.close()
    conn.close()

    extracted_dict = {(row[0], row[1]): row[2] for row in extracted_data}

    return extracted_dict
