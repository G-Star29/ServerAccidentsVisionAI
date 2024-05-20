import pandas as pd
import psycopg2
from psycopg2 import sql

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

# Получаем данные из таблицы columns_name_table
cur.execute("SELECT * FROM accidentvisionai.columns_name_table")
columns_name_table = dict(cur.fetchall())

# Загружаем данные из CSV файла
output_df = pd.read_csv('data/csv/output_modified_test_final.csv')

# Пример данных для вставки (только для формирования структуры)
data_for_return = {
    "col_4": 0,
    "col_5": 0, "col_6": 0, "col_7": 0, "col_8": 0, "col_9": 1,
    "col_10": 0, "col_11": 0, "col_12": 1, "col_13": 0, "col_14": 0,
    "col_15": 0, "col_16": 1, "col_17": 0, "col_18": 0, "col_20": 0,
    "col_21": 0, "col_22": 0, "col_23": 1, "col_24": 0, "col_25": 0,
    "col_80": 0, "col_81": 0, "col_82": 0, "col_83": 0, "col_84": 0,
    "col_85": 0, "col_86": 0, "col_87": 0, "col_88": 0, "col_89": 0,
    "col_90": 0, "col_91": 0, "col_92": 0, "col_93": 0, "col_94": 0,
    "col_95": 0, "col_96": 0, "col_97": 0, "col_98": 0, "col_99": 0,
    "col_100": 0, "col_101": 0, "col_102": 0, "col_103": 0, "col_104": 0,
    "col_105": 0, "col_106": 0, "col_107": 0, "col_108": 0, "col_109": 0,
    "col_110": 0, "col_111": 0, "col_112": 0, "col_113": 0, "col_114": 0,
    "col_115": 0, "col_116": 0, "col_117": 0, "col_118": 0, "col_119": 0,
    "col_120": 0, "col_121": 0, "col_122": 0, "col_123": 0, "col_124": 0,
    "col_125": 0, "col_126": 0, "col_127": 0, "col_128": 0, "col_129": 0,
    "col_130": 0, "col_131": 0, "col_132": 0, "col_133": 0, "col_134": 0,
    "col_135": 0, "col_136": 0, "col_137": 0, "col_138": 0, "col_139": 0,
    "col_140": 0, "col_141": 0, "col_142": 0, "col_143": 0, "col_144": 0,
    "col_145": 0, "col_146": 0, "col_147": 0, "col_148": 0, "col_149": 0,
    "col_150": 0, "col_151": 0, "col_152": 0, "col_153": 0, "col_154": 0,
    "col_155": True
}

# Удаляем существующие данные из таблицы
cur.execute("DELETE FROM accidentvisionai.new_indications")

# Получаем ID из таблицы coords_and_nearby
cur.execute("SELECT col_1 FROM accidentvisionai.coords_and_nearby_new")
coords_ids = cur.fetchall()

# Для каждой записи в coords_and_nearby_new берем соответствующие данные из CSV и вставляем в data_for_prediction_table
for coords_id in coords_ids:
    row = output_df[output_df[columns_name_table['col_1']] == coords_id[0]]
    if not row.empty:
        data_for_current_state = row.iloc[0].to_dict()
        combined_data = {col: data_for_current_state[columns_name_table[col]] for col in data_for_return.keys() if
                         columns_name_table[col] in data_for_current_state}

        # Формируем значения для вставки
        data_values = list(combined_data.values()) + [coords_id[0]]

        # SQL-запрос для вставки данных в таблицу predictions
        insert_predictions_query = sql.SQL("""
        INSERT INTO accidentvisionai.new_indications (
            {fields}, coords_and_nearby
        ) VALUES ({values})
        """).format(
            fields=sql.SQL(', ').join(map(sql.Identifier, combined_data.keys())),
            values=sql.SQL(', ').join(sql.Placeholder() * (len(data_values)))
        )

        # Выполняем вставку данных
        cur.execute(insert_predictions_query, data_values)

# Сохранение изменений и закрытие соединения
conn.commit()
cur.close()
conn.close()

print("Table data_for_prediction_table has been populated with data based on coords_and_nearby_new.")
