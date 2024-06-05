from datetime import datetime

import psycopg2
import requests
import json
import pandas as pd
from dateutil.relativedelta import relativedelta
import warnings

from .dataMod import mod_parsed_data
from .createTableCoordsNew import create_table_coords_new
from .mergeNewDataFromGIBDDtoEducation import merge_new_data_from_GIBDD_to_Education
from .mergeDataToIndications import merge_data_to_indications

warnings.filterwarnings("ignore")

def get_last_date_from_db():
    dbname = 'accidentsvisionai'
    user = 'postgres'
    password = 'Nikita232398'

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password
    )
    cur = conn.cursor()

    # SQL-запрос для получения самой последней даты
    query = """
    SELECT col_80 AS year,
           CASE 
               WHEN col_92 = 1 THEN 12
               WHEN col_91 = 1 THEN 11
               WHEN col_90 = 1 THEN 10
               WHEN col_89 = 1 THEN 9
               WHEN col_88 = 1 THEN 8
               WHEN col_87 = 1 THEN 7
               WHEN col_86 = 1 THEN 6
               WHEN col_85 = 1 THEN 5
               WHEN col_84 = 1 THEN 4
               WHEN col_83 = 1 THEN 3
               WHEN col_82 = 1 THEN 2
               WHEN col_81 = 1 THEN 1
               ELSE NULL
           END AS month,
           CASE 
               WHEN col_123 = 1 THEN 31
               WHEN col_122 = 1 THEN 30
               WHEN col_121 = 1 THEN 29
               WHEN col_120 = 1 THEN 28
               WHEN col_119 = 1 THEN 27
               WHEN col_118 = 1 THEN 26
               WHEN col_117 = 1 THEN 25
               WHEN col_116 = 1 THEN 24
               WHEN col_115 = 1 THEN 23
               WHEN col_114 = 1 THEN 22
               WHEN col_113 = 1 THEN 21
               WHEN col_112 = 1 THEN 20
               WHEN col_111 = 1 THEN 19
               WHEN col_110 = 1 THEN 18
               WHEN col_109 = 1 THEN 17
               WHEN col_108 = 1 THEN 16
               WHEN col_107 = 1 THEN 15
               WHEN col_106 = 1 THEN 14
               WHEN col_105 = 1 THEN 13
               WHEN col_104 = 1 THEN 12
               WHEN col_103 = 1 THEN 11
               WHEN col_102 = 1 THEN 10
               WHEN col_101 = 1 THEN 9
               WHEN col_100 = 1 THEN 8
               WHEN col_99 = 1 THEN 7
               WHEN col_98 = 1 THEN 6
               WHEN col_97 = 1 THEN 5
               WHEN col_96 = 1 THEN 4
               WHEN col_95 = 1 THEN 3
               WHEN col_94 = 1 THEN 2
               WHEN col_93 = 1 THEN 1
               ELSE NULL
           END AS day,
           CASE 
               WHEN col_153 = 1 THEN 23
               WHEN col_152 = 1 THEN 22
               WHEN col_151 = 1 THEN 21
               WHEN col_150 = 1 THEN 20
               WHEN col_149 = 1 THEN 19
               WHEN col_148 = 1 THEN 18
               WHEN col_147 = 1 THEN 17
               WHEN col_146 = 1 THEN 16
               WHEN col_145 = 1 THEN 15
               WHEN col_144 = 1 THEN 14
               WHEN col_143 = 1 THEN 13
               WHEN col_142 = 1 THEN 12
               WHEN col_141 = 1 THEN 11
               WHEN col_140 = 1 THEN 10
               WHEN col_139 = 1 THEN 9
               WHEN col_138 = 1 THEN 8
               WHEN col_137 = 1 THEN 7
               WHEN col_136 = 1 THEN 6
               WHEN col_135 = 1 THEN 5
               WHEN col_134 = 1 THEN 4
               WHEN col_133 = 1 THEN 3
               WHEN col_132 = 1 THEN 2
               WHEN col_131 = 1 THEN 1
               ELSE NULL
           END AS hour
    FROM accidentvisionai.data_for_education_table
    ORDER BY col_80 DESC, 
             CASE 
                 WHEN col_92 = 1 THEN 12
                 WHEN col_91 = 1 THEN 11
                 WHEN col_90 = 1 THEN 10
                 WHEN col_89 = 1 THEN 9
                 WHEN col_88 = 1 THEN 8
                 WHEN col_87 = 1 THEN 7
                 WHEN col_86 = 1 THEN 6
                 WHEN col_85 = 1 THEN 5
                 WHEN col_84 = 1 THEN 4
                 WHEN col_83 = 1 THEN 3
                 WHEN col_82 = 1 THEN 2
                 WHEN col_81 = 1 THEN 1
                 ELSE 0
             END DESC,
             CASE 
                 WHEN col_123 = 1 THEN 31
                 WHEN col_122 = 1 THEN 30
                 WHEN col_121 = 1 THEN 29
                 WHEN col_120 = 1 THEN 28
                 WHEN col_119 = 1 THEN 27
                 WHEN col_118 = 1 THEN 26
                 WHEN col_117 = 1 THEN 25
                 WHEN col_116 = 1 THEN 24
                 WHEN col_115 = 1 THEN 23
                 WHEN col_114 = 1 THEN 22
                 WHEN col_113 = 1 THEN 21
                 WHEN col_112 = 1 THEN 20
                 WHEN col_111 = 1 THEN 19
                 WHEN col_110 = 1 THEN 18
                 WHEN col_109 = 1 THEN 17
                 WHEN col_108 = 1 THEN 16
                 WHEN col_107 = 1 THEN 15
                 WHEN col_106 = 1 THEN 14
                 WHEN col_105 = 1 THEN 13
                 WHEN col_104 = 1 THEN 12
                 WHEN col_103 = 1 THEN 11
                 WHEN col_102 = 1 THEN 10
                 WHEN col_101 = 1 THEN 9
                 WHEN col_100 = 1 THEN 8
                 WHEN col_99 = 1 THEN 7
                 WHEN col_98 = 1 THEN 6
                 WHEN col_97 = 1 THEN 5
                 WHEN col_96 = 1 THEN 4
                 WHEN col_95 = 1 THEN 3
                 WHEN col_94 = 1 THEN 2
                 WHEN col_93 = 1 THEN 1
                 ELSE 0
             END DESC,
             CASE 
                 WHEN col_153 = 1 THEN 23
                 WHEN col_152 = 1 THEN 22
                 WHEN col_151 = 1 THEN 21
                 WHEN col_150 = 1 THEN 20
                 WHEN col_149 = 1 THEN 19
                 WHEN col_148 = 1 THEN 18
                 WHEN col_147 = 1 THEN 17
                 WHEN col_146 = 1 THEN 16
                 WHEN col_145 = 1 THEN 15
                 WHEN col_144 = 1 THEN 14
                 WHEN col_143 = 1 THEN 13
                 WHEN col_142 = 1 THEN 12
                 WHEN col_141 = 1 THEN 11
                 WHEN col_140 = 1 THEN 10
                 WHEN col_139 = 1 THEN 9
                 WHEN col_138 = 1 THEN 8
                 WHEN col_137 = 1 THEN 7
                 WHEN col_136 = 1 THEN 6
                 WHEN col_135 = 1 THEN 5
                 WHEN col_134 = 1 THEN 4
                 WHEN col_133 = 1 THEN 3
                 WHEN col_132 = 1 THEN 2
                 WHEN col_131 = 1 THEN 1
                 ELSE 0
             END DESC
    LIMIT 1;
    """

    cur.execute(query)
    last_date = cur.fetchone()

    # Закрываем соединение с базой данных
    cur.close()
    conn.close()

    print(f"[PARSE] Последняя дата в базе данных: {last_date[0]}-{last_date[1]:02}-{last_date[2]:02} {last_date[3]:02}:00")
    return last_date


def try_to_parse_new_data_from_gibdd():
    last_date = get_last_date_from_db()

    last_date = datetime(last_date[0], last_date[1], last_date[2], last_date[3])

    date_for_check = last_date + relativedelta(months=1)

    new_month = date_for_check.month
    new_year = date_for_check.year

    url = 'http://stat.gibdd.ru/map/getDTPCardData'

    data = {
        "data": f'{{"date":["MONTHS:{new_month}.{new_year}"],"ParReg":"50","order":{{"type":"1","fieldName":"dat"}},'
                f'"reg":"50401","ind":"1","st":"1","en":"16"}}'
    }

    response = requests.post(url, json=data)

    if response.status_code == 200:

        if response.json().get('data') == "":
            print("[PARSER] Данные актуальны")
            return 1

        filename = 'data/json/response.json'

        try:

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=4)
            print('[DATA PARSE] Данные успешно сохранены в JSON.')
            with open(filename, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            data = json.loads(json_data["data"])

            accidents = []
            for entry in data["tab"]:
                accident = {
                    "id": entry["KartId"],
                    "latitude": entry["infoDtp"]["COORD_W"],
                    "longitude": entry["infoDtp"]["COORD_L"],
                    "weather": entry["infoDtp"]["s_pog"][0],
                    "datetime": f"{entry['date']} {entry['Time']}",
                    "ligth": entry["infoDtp"]["osv"],
                    "road_conditions": "; ".join(entry["infoDtp"]["ndu"]),
                    "nearby": "; ".join(entry["infoDtp"]["OBJ_DTP"])
                }
                accidents.append(accident)

            df = pd.DataFrame(accidents)

            output_csv_file_path = 'data/csv/parsed_output.csv'
            df.to_csv(output_csv_file_path, index=False)

            print("[PARSE] Данные успешно сохранены в parsed_output.csv")

        except Exception as e:
            print(f'[PARSE] Ошибка! Невозможно сохранить данные: {str(e)}')
            return 1


    else:
        print(f'[PARSE] Ошибка в отправке POST запроса. Status code: {response.status_code}')
        print(f'[PARSE] Текст запроса: {response.text}')
        return 1

    mod_parsed_data()
    create_table_coords_new()
    merge_data_to_indications()
    merge_new_data_from_GIBDD_to_Education()

