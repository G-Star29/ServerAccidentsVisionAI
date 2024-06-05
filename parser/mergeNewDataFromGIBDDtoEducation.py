import pandas as pd
import psycopg2
from psycopg2 import sql
from geopy.distance import geodesic
import random
import numpy as np
from collections import namedtuple

def merge_new_data_from_GIBDD_to_Education():
    # Параметры подключения
    dbname = 'accidentsvisionai'
    user = 'postgres'
    password = 'Nikita232398'

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

    # Получаем последний существующий ID
    cur.execute("SELECT MAX(col_1) FROM accidentvisionai.data_for_education_table_test")
    last_id = cur.fetchone()[0]
    if last_id is None:
        last_id = 0

    # Создаем новые уникальные идентификаторы
    merged_df['col_1'] = range(last_id + 1, last_id + 1 + len(merged_df))

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

    # Функция для обновления или вставки новых координат в таблицу coords_and_nearby
    def upsert_coords_and_nearby(new_row):
        cur.execute("""
            SELECT col_1 FROM accidentvisionai.coords_and_nearby
            WHERE col_2 = %s AND col_3 = %s
        """, (new_row.col_2, new_row.col_3))

        existing_coords = cur.fetchone()

        if existing_coords:
            # Обновляем существующую запись
            update_query = sql.SQL("""
                UPDATE accidentvisionai.coords_and_nearby
                SET {}
                WHERE col_1 = %s
            """).format(sql.SQL(', ').join(
                sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder()]) for k in
                coords_and_nearby_new_columns
                if k != 'col_1'
            ))

            cur.execute(update_query,
                        [getattr(new_row, k) for k in coords_and_nearby_new_columns if k != 'col_1'] + [existing_coords[0]])
        else:
            # Вставляем новую запись
            insert_query = sql.SQL("""
                INSERT INTO accidentvisionai.coords_and_nearby_test ({})
                VALUES ({})
            """).format(
                sql.SQL(', ').join(map(sql.Identifier, coords_and_nearby_new_columns)),
                sql.SQL(', ').join(sql.Placeholder() * len(coords_and_nearby_new_columns))
            )

            cur.execute(insert_query, [getattr(new_row, k) for k in coords_and_nearby_new_columns])

    # Обрабатываем каждую новую координату
    for new_row in merged_df.itertuples(index=False, name='Row'):
        # Добавляем или обновляем координаты в таблице coords_and_nearby
        upsert_coords_and_nearby(new_row)

    select_columns_query = "SELECT column_number, column_name FROM accidentvisionai.columns_name_table;"
    cur.execute(select_columns_query)
    columns_data = cur.fetchall()

    columns_dict = {col_num: col_name for col_num, col_name in columns_data}

    merged_df.rename(columns=columns_dict, inplace=True)

    print("[PARSE] Данные успешно добавлены в таблицу data_for_education_table и coords_and_nearby.")

    # Генерация "безаварийных случаев" и вставка их в таблицу data_for_education_table_test
    synthetic_non_accidents = generate_synthetic_non_accidents(merged_df)
    last_id = merged_df.tail(1)['id'].values[0] + 1
    new_id_for_synthetic_non_accidents = list(range(last_id, last_id + len(synthetic_non_accidents)))
    synthetic_non_accidents['id'] = new_id_for_synthetic_non_accidents
    for row in synthetic_non_accidents.itertuples(index=False, name=None):
        cur.execute(insert_query, row)

    # Сохраняем изменения и закрываем соединение
    conn.commit()
    cur.close()
    conn.close()

    print('[PARSE] Данные об "безаварийных" случаях добавлены в data_for_education_table')

def load_data(file_path):
    return pd.read_csv(file_path)

def calculate_distance(lat1, lon1, lat2, lon2):
    return geodesic((lat1, lon1), (lat2, lon2)).meters

def generate_synthetic_non_accidents(df, num_samples=10000):
    excluded_columns = [
        'Регулируемый пешеходный переход, расположенный на участке улицы или дороги, проходящей вдоль территории школы или иного детского учреждения',
        'Нерегулируемое пересечение с круговым движением',
        'Многоквартирные жилые дома',
        'Зоны отдыха',
        'Тоннель',
        'Регулируемый перекрёсток',
        'АЗС',
        'Производственное предприятие',
        'Регулируемый ж/д переезд без дежурного',
        'Выезд с прилегающей территории',
        'Одиночный торговый объект, являющийся местом притяжения транспорта и (или) пешеходов',
        'Нерегулируемый пешеходный переход',
        'Административные здания',
        'Спортивные и развлекательные объекты',
        'Регулируемый ж/д переезд с дежурным',
        'Надземный пешеходный переход',
        'Подземный пешеходный переход',
        'Школа либо иная детская (в т.ч. дошкольная) организация',
        'Иной объект',
        'Эстакада, путепровод',
        'Кладбище',
        'Объект строительства',
        'Территориальное подразделение МВД России (либо его структурное подразделение)',
        'Пешеходная зона',
        'Автостоянка (отделенная от проезжей части)',
        'Мост, эстакада, путепровод',
        'Медицинские (лечебные) организации',
        'Автостоянка (не отделённая от проезжей части)',
        'Объект торговли, общественного питания на автодороге вне НП',
        'Подход к мосту, эстакаде, путепроводу',
        'Нерегулируемый пешеходный переход, расположенный на участке улицы или дороги, проходящей вдоль территории школы или иной детской организации',
        'Автовокзал (автостанция)',
        'Остановка общественного транспорта',
        'Иное образовательное учреждение',
        'Регулируемый пешеходный переход, расположенный на участке улицы или дороги, проходящей вдоль территории школы или иной детской организации',
        'Регулируемый перекресток',
        'Иная образовательная организация',
        'Тротуар, пешеходная дорожка',
        'Нерегулируемый ж/д переезд',
        'Крупный торговый объект (являющийся объектом массового тяготения пешеходов и (или) транспорта)',
        'Остановка трамвая',
        'Школа либо иное детское (в т.ч. дошкольное) учреждение',
        'Лечебные учреждения',
        'Нерегулируемый перекрёсток неравнозначных улиц (дорог)',
        'Жилые дома индивидуальной застройки',
        'Остановка маршрутного такси',
        'Мост',
        'Внутридворовая территория',
        'Аэропорт, ж/д вокзал (ж/д станция), речной или морской порт (пристань)',
        'Нерегулируемый перекрёсток равнозначных улиц (дорог)',
        'Нерегулируемый пешеходный переход, расположенный на участке улицы или дороги, проходящей вдоль территории школы или иного детского учреждения',
        'Регулируемый пешеходный переход',
        'Нерегулируемый перекрёсток',
        'Объект (здание, сооружение) религиозного культа'
    ]
    # Список для хранения новых синтетических строк данных
    new_data_rows = []
    # Множество для отслеживания уже использованных аварий
    used_accidents = set()
    # Удаляем строки с пустыми координатами
    df = df.dropna(subset=['latitude', 'longitude'])
    # Получаем индексы строк с авариями
    accident_indices = df[df['accident_occurred'] == 1].index.tolist()
    # Перемешиваем индексы аварий
    random.shuffle(accident_indices)

    for idx in accident_indices:
        if len(used_accidents) >= num_samples:
            break
        if idx not in used_accidents:
            accident_row = df.loc[idx]
            # Находим потенциальные неаварийные строки, которые не совпадают по году, месяцу, дню и часу
            potential_matches = df[(df['year'] != accident_row['year']) |
                                   (df[['month_' + str(i) for i in range(1, 13)]] != accident_row[
                                       ['month_' + str(i) for i in range(1, 13)]]).any(axis=1) |
                                   (df[['day_' + str(i) for i in range(1, 32)]] != accident_row[
                                       ['day_' + str(i) for i in range(1, 32)]]).any(axis=1) |
                                   (df[['hour_' + str(i) for i in range(24)]] != accident_row[
                                       ['hour_' + str(i) for i in range(24)]]).any(axis=1)]

            # Перемешиваем потенциальные строки
            potential_matches = potential_matches.sample(frac=1)

            for _, non_accident_row in potential_matches.iterrows():
                distance = calculate_distance(accident_row['latitude'], accident_row['longitude'],
                                              non_accident_row['latitude'], non_accident_row['longitude'])
                # Проверяем, что расстояние между точками больше 100 метров
                if distance >= 100:
                    # Создаем новую строку, где координаты берутся из неаварийной строки, а некоторые поля из аварийной
                    new_row = {col: non_accident_row[col] if col not in excluded_columns else accident_row[col] for col in df.columns}
                    new_row['latitude'] = non_accident_row['latitude']
                    new_row['longitude'] = non_accident_row['longitude']
                    # Указываем, что это не авария
                    new_row['accident_occurred'] = 0
                    # Добавляем новую строку в список
                    new_data_rows.append(new_row)
                    # Отмечаем аварийную строку как использованную
                    used_accidents.add(idx)
                    break
    # Сохраняем порядок столбцов
    final_columns = df.columns.tolist()
    # Возвращаем DataFrame с новыми данными
    return pd.DataFrame(new_data_rows, columns=final_columns)


