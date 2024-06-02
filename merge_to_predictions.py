import psycopg2
import pandas as pd

def merge_to_predictions_table(data_of_current_state, for_current_state=True):
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

    # Пример данных для вставки
    data_for_return = {
        "col_5": 0,
        "col_6": 0,
        "col_7": 0,
        "col_8": 0,
        "col_9": 1,
        "col_10": 0,
        "col_11": 0,
        "col_12": 1,
        "col_13": 0,
        "col_14": 0,
        "col_15": 0,
        "col_16": 1,
        "col_17": 0,
        "col_18": 0,
        "col_20": 0,
        "col_21": 0,
        "col_22": 0,
        "col_23": 1,
        "col_24": 0,
        "col_25": 0,
        "col_80": 0,
        "col_81": 0,
        "col_82": 0,
        "col_83": 0,
        "col_84": 0,
        "col_85": 0,
        "col_86": 0,
        "col_87": 0,
        "col_88": 0,
        "col_89": 0,
        "col_90": 0,
        "col_91": 0,
        "col_92": 0,
        "col_93": 0,
        "col_94": 0,
        "col_95": 0,
        "col_96": 0,
        "col_97": 0,
        "col_98": 0,
        "col_99": 0,
        "col_100": 0,
        "col_101": 0,
        "col_102": 0,
        "col_103": 0,
        "col_104": 0,
        "col_105": 0,
        "col_106": 0,
        "col_107": 0,
        "col_108": 0,
        "col_109": 0,
        "col_110": 0,
        "col_111": 0,
        "col_112": 0,
        "col_113": 0,
        "col_114": 0,
        "col_115": 0,
        "col_116": 0,
        "col_117": 0,
        "col_118": 0,
        "col_119": 0,
        "col_120": 0,
        "col_121": 0,
        "col_122": 0,
        "col_123": 0,
        "col_124": 0,
        "col_125": 0,
        "col_126": 0,
        "col_127": 0,
        "col_128": 0,
        "col_129": 0,
        "col_130": 0,
        "col_131": 0,
        "col_132": 0,
        "col_133": 0,
        "col_134": 0,
        "col_135": 0,
        "col_136": 0,
        "col_137": 0,
        "col_138": 0,
        "col_139": 0,
        "col_140": 0,
        "col_141": 0,
        "col_142": 0,
        "col_143": 0,
        "col_144": 0,
        "col_145": 0,
        "col_146": 0,
        "col_147": 0,
        "col_148": 0,
        "col_149": 0,
        "col_150": 0,
        "col_151": 0,
        "col_152": 0,
        "col_153": 0,
        "col_154": 0,
        "col_155": True
    }

    combined_data = dict(zip(data_for_return.keys(), data_of_current_state.values()))

    if for_current_state:
        # SQL-запрос для вставки данных в таблицу predictions
        insert_predictions_query = """
        INSERT INTO accidentvisionai.data_for_prediction_table (
            col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14,
            col_15, col_16, col_17, col_18, col_20, col_21, col_22, col_23, col_24, col_25,
            col_80, col_81, col_82, col_83, col_84, col_85, col_86, col_87, col_88, col_89,
            col_90, col_91, col_92, col_93, col_94, col_95, col_96, col_97, col_98, col_99,
            col_100, col_101, col_102, col_103, col_104, col_105, col_106, col_107, col_108, col_109,
            col_110, col_111, col_112, col_113, col_114, col_115, col_116, col_117, col_118, col_119,
            col_120, col_121, col_122, col_123, col_124, col_125, col_126, col_127, col_128, col_129,
            col_130, col_131, col_132, col_133, col_134, col_135, col_136, col_137, col_138, col_139,
            col_140, col_141, col_142, col_143, col_144, col_145, col_146, col_147, col_148, col_149,
            col_150, col_151, col_152, col_153, col_154, col_155, coords_and_nearby
        ) VALUES ({});
        """.format(', '.join(['%s'] * (len(combined_data) + 1)))

        # Получение ID из таблицы coords_and_nearby и вставка данных в таблицу predictions
        cur.execute("DELETE FROM accidentvisionai.data_for_prediction_table")
        cur.execute("SELECT col_1 FROM accidentvisionai.coords_and_nearby")
        coords_ids = cur.fetchall()

        for coords_id in coords_ids:
            data_values = list(combined_data.values()) + [coords_id[0]]
            cur.execute(insert_predictions_query, data_values)

        # Сохранение изменений и закрытие соединения
        conn.commit()
        cur.close()
        conn.close()

        print("[DATA MERGE] Таблица предсказаний была заполнена данными на основе coords_and_nearby.")

    else:
        # Получение данных из таблицы coords_and_nearby
        cur.execute("SELECT * FROM accidentvisionai.coords_and_nearby")
        coords_data = cur.fetchall()
        coords_columns = [desc[0] for desc in cur.description]

        # Удаление col_1 из списка столбцов
        coords_columns.remove('col_1')

        # Создание списка для накопления данных
        data_list = []

        for row in coords_data:
            coords_dict = dict(zip(coords_columns, row[1:]))
            # Комбинируем данные
            combined_row = {**combined_data}
            for col in coords_columns:
                if col in combined_row:
                    combined_row[col] = coords_dict[col]
                else:
                    combined_row[col] = coords_dict[col]
            data_list.append(combined_row)

        # Создание DataFrame из списка данных
        all_columns = sorted(list(combined_data.keys()) + coords_columns, key=lambda x: int(x.split('_')[1]))
        dataframe = pd.DataFrame(data_list, columns=all_columns)

        cur.close()
        conn.close()

        print("[DATA MERGE] Данные были подготовлены для предсказаний без вставки в БД.")
        return dataframe
