import pandas as pd
import psycopg2
from psycopg2 import sql
import numpy as np
from math import radians, cos, sin, sqrt, atan2


def merge_new_data_from_GIBDD_to_Education():
    # Параметры подключения
    dbname = 'accidentsvisionai'
    user = 'postgres'
    password = 'Nikita232398'

    # Функция для вычисления расстояния между двумя координатами по формуле Хаверсина
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371000  # Радиус Земли в метрах
        phi1, phi2 = radians(lat1), radians(lat2)
        delta_phi = radians(lat2 - lat1)
        delta_lambda = radians(lon2 - lon1)
        a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return R * c

    # Функция для извлечения даты и времени из строки данных
    def extract_datetime(row):
        try:
            year = row['col_80']
            month = next((month for month in range(1, 13) if row.get(f'col_{81 + month - 1}', 0) == 1), None)
            day = next((day for day in range(1, 32) if row.get(f'col_{93 + day - 1}', 0) == 1), None)
            hour = next((hour for hour in range(24) if row.get(f'col_{131 + hour}', 0) == 1), None)
            return (year, month, day, hour)
        except KeyError as e:
            print(f"KeyError: {e} in row {row.name}")
            return None

    # Функция для проверки совпадения даты и времени
    def is_same_datetime(row1, row2):
        dt1 = extract_datetime(row1)
        dt2 = extract_datetime(row2)
        return dt1 is not None and dt2 is not None and dt1 == dt2

    # Подключаемся к базе данных PostgreSQL
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password
    )
    cur = conn.cursor()

    # Извлекаем данные из таблицы coords_and_nearby_new
    cur.execute("SELECT * FROM accidentvisionai.coords_and_nearby_new")
    coords_and_nearby_new_data = cur.fetchall()

    # Получаем имена столбцов для таблицы coords_and_nearby_new
    coords_and_nearby_new_columns = [desc[0] for desc in cur.description]

    # Преобразуем данные в DataFrame
    coords_and_nearby_new_df = pd.DataFrame(coords_and_nearby_new_data, columns=coords_and_nearby_new_columns)

    # Извлекаем данные из таблицы new_indications
    cur.execute("SELECT * FROM accidentvisionai.new_indications")
    new_indications_data = cur.fetchall()

    # Получаем имена столбцов для таблицы new_indications
    new_indications_columns = [desc[0] for desc in cur.description]

    # Преобразуем данные в DataFrame
    new_indications_df = pd.DataFrame(new_indications_data, columns=new_indications_columns)

    # Объединяем данные на основе внешнего ключа coords_and_nearby
    merged_df = pd.merge(coords_and_nearby_new_df, new_indications_df, left_on='col_1', right_on='coords_and_nearby')

    # Удаляем лишние столбцы после объединения
    merged_df.drop(columns=['coords_and_nearby', 'id'], inplace=True)

    # Преобразуем значения в столбце col_155 в тип boolean
    merged_df['col_155'] = merged_df['col_155'].astype(bool)

    # Преобразуем типы данных для других столбцов в соответствии со структурой data_for_education_table_test
    for col in merged_df.columns:
        if col not in ['col_2', 'col_3', 'col_155']:
            merged_df[col] = merged_df[col].astype(int)
        elif col in ['col_2', 'col_3']:
            merged_df[col] = merged_df[col].astype(float)

    # Создаем список столбцов для вставки в таблицу data_for_education_table_test
    data_for_education_columns = merged_df.columns.tolist()

    # Создаем запрос для вставки данных
    insert_query = sql.SQL("INSERT INTO accidentvisionai.data_for_education_table_test ({}) VALUES ({})").format(
        sql.SQL(', ').join(map(sql.Identifier, data_for_education_columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(data_for_education_columns))
    )

    # Вставляем данные в таблицу data_for_education_table_test
    for row in merged_df.itertuples(index=False, name=None):
        cur.execute(insert_query, row)

    # Извлекаем данные из таблицы coords_and_nearby и indications_table и объединяем их
    cur.execute("""
        SELECT cn.*, it.*
        FROM accidentvisionai.coords_and_nearby cn
        JOIN accidentvisionai.indications_table it
        ON cn.col_1 = it.id
    """)
    coords_and_indications_data = cur.fetchall()

    # Получаем имена столбцов для объединенных данных
    coords_and_indications_columns = [desc[0] for desc in cur.description]

    # Преобразуем данные в DataFrame
    coords_and_indications_df = pd.DataFrame(coords_and_indications_data, columns=coords_and_indications_columns)

    # Обрабатываем каждую новую координату
    for index, new_row in merged_df.iterrows():
        lat1, lon1 = new_row['col_2'], new_row['col_3']
        found_match = False

        for index2, old_row in coords_and_indications_df.iterrows():
            lat2, lon2 = old_row['col_2'], old_row['col_3']

            if haversine(lat1, lon1, lat2, lon2) > 200:
                if is_same_datetime(new_row, old_row):
                    try:
                        # Добавляем новую запись в таблицу data_for_education_table_test как безаварийный случай
                        new_row_data = {**old_row.to_dict(), **new_row.to_dict()}
                        new_row_data['col_4'] = 0  # помечаем как безаварийный случай

                        # Преобразуем значения в boolean и int, где это необходимо
                        new_row_data = {
                            k: bool(v) if k == 'col_155' else int(v) if isinstance(v, np.integer) else float(
                                v) if isinstance(v, np.float64) else v for k, v in new_row_data.items()}

                        # Удаляем col_1, чтобы позволить автоинкремент
                        new_row_data.pop('col_1')

                        cur.execute(insert_query, list(new_row_data.values()))
                        found_match = True
                        break
                    except Exception as e:
                        print(f"[PARSE] Ошибка при обработке координат {new_row['col_1']} и {old_row['col_1']}: {e}")
                        return 1
        if not found_match:
            print(f"[PARSE] Недостаточно данных для координаты {new_row['col_1']}")
            return 1

    # Сохраняем изменения и закрываем соединение
    conn.commit()
    cur.close()
    conn.close()

    print("[PARSE] Данные успешно добавлены в таблицу data_for_education_table_test.")
    return 0
