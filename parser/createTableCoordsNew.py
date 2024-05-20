import pandas as pd
import psycopg2
from psycopg2 import sql, extras

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

# Получаем данные из таблицы columns_name_table
cur.execute("SELECT * FROM accidentvisionai.columns_name_table")
columns_mapping = dict(cur.fetchall())

# Фильтруем только нужные столбцы для новой таблицы
columns_needed = [
    "col_1", "col_2", "col_3",
    "col_26", "col_27", "col_28", "col_29", "col_30", "col_31",
    "col_32", "col_33", "col_34", "col_35", "col_36", "col_37",
    "col_38", "col_39", "col_40", "col_41", "col_42", "col_43",
    "col_44", "col_45", "col_46", "col_47", "col_48", "col_49",
    "col_50", "col_51", "col_52", "col_53", "col_54", "col_55",
    "col_56", "col_57", "col_58", "col_59", "col_60", "col_61",
    "col_62", "col_63", "col_64", "col_65", "col_66", "col_67",
    "col_68", "col_69", "col_70", "col_71", "col_72", "col_73",
    "col_74", "col_75", "col_76", "col_77", "col_78", "col_79"
]

filtered_mapping = {col: columns_mapping[col] for col in columns_needed}

# Создаем таблицу coords_and_nearby_new
create_coords_and_nearby_query = """
CREATE TABLE IF NOT EXISTS accidentvisionai.coords_and_nearby_new (
    col_1 INT PRIMARY KEY,
    col_2 FLOAT,
    col_3 FLOAT,
    col_26 INT,
    col_27 INT,
    col_28 INT,
    col_29 INT,
    col_30 INT,
    col_31 INT,
    col_32 INT,
    col_33 INT,
    col_34 INT,
    col_35 INT,
    col_36 INT,
    col_37 INT,
    col_38 INT,
    col_39 INT,
    col_40 INT,
    col_41 INT,
    col_42 INT,
    col_43 INT,
    col_44 INT,
    col_45 INT,
    col_46 INT,
    col_47 INT,
    col_48 INT,
    col_49 INT,
    col_50 INT,
    col_51 INT,
    col_52 INT,
    col_53 INT,
    col_54 INT,
    col_55 INT,
    col_56 INT,
    col_57 INT,
    col_58 INT,
    col_59 INT,
    col_60 INT,
    col_61 INT,
    col_62 INT,
    col_63 INT,
    col_64 INT,
    col_65 INT,
    col_66 INT,
    col_67 INT,
    col_68 INT,
    col_69 INT,
    col_70 INT,
    col_71 INT,
    col_72 INT,
    col_73 INT,
    col_74 INT,
    col_75 INT,
    col_76 INT,
    col_77 INT,
    col_78 INT,
    col_79 INT
);
"""
cur.execute(create_coords_and_nearby_query)

# Считываем данные из выходного CSV-файла
output_df = pd.read_csv('output_modified_test_final.csv')

# Выбираем только нужные столбцы из выходной таблицы
output_df = output_df[[filtered_mapping[col] for col in filtered_mapping]]

# Переименовываем столбцы для соответствия структуре таблицы PostgreSQL
output_df = output_df.rename(columns={v: k for k, v in filtered_mapping.items()})

# Формируем SQL запрос для вставки данных
insert_query = sql.SQL("""
INSERT INTO accidentvisionai.coords_and_nearby_new (
    {fields}
) VALUES %s
""").format(fields=sql.SQL(', ').join(map(sql.Identifier, filtered_mapping.keys())))

# Преобразуем DataFrame в список кортежей
tuples = [tuple(x) for x in output_df.to_numpy()]

# Выполняем вставку данных с использованием execute_values
extras.execute_values(cur, insert_query.as_string(cur), tuples)

# Сохраняем изменения и закрываем соединение
conn.commit()
cur.close()
conn.close()

print("Table coords_and_nearby_new has been created and populated with data.")