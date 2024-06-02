import psycopg2

# Параметры подключения
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

# Создаем таблицу predictions
create_predictions_table_query = """
CREATE TABLE IF NOT EXISTS accidentvisionai.indications_table (
    id SERIAL PRIMARY KEY,
    col_5 INT,
    col_6 INT,
    col_7 INT,
    col_8 INT,
    col_9 INT,
    col_10 INT,
    col_11 INT,
    col_12 INT,
    col_13 INT,
    col_14 INT,
    col_15 INT,
    col_16 INT,
    col_17 INT,
    col_18 INT,
    col_20 INT,
    col_21 INT,
    col_22 INT,
    col_23 INT,
    col_24 INT,
    col_25 INT,
    col_80 INT,
    col_81 INT,
    col_82 INT,
    col_83 INT,
    col_84 INT,
    col_85 INT,
    col_86 INT,
    col_87 INT,
    col_88 INT,
    col_89 INT,
    col_90 INT,
    col_91 INT,
    col_92 INT,
    col_93 INT,
    col_94 INT,
    col_95 INT,
    col_96 INT,
    col_97 INT,
    col_98 INT,
    col_99 INT,
    col_100 INT,
    col_101 INT,
    col_102 INT,
    col_103 INT,
    col_104 INT,
    col_105 INT,
    col_106 INT,
    col_107 INT,
    col_108 INT,
    col_109 INT,
    col_110 INT,
    col_111 INT,
    col_112 INT,
    col_113 INT,
    col_114 INT,
    col_115 INT,
    col_116 INT,
    col_117 INT,
    col_118 INT,
    col_119 INT,
    col_120 INT,
    col_121 INT,
    col_122 INT,
    col_123 INT,
    col_124 INT,
    col_125 INT,
    col_126 INT,
    col_127 INT,
    col_128 INT,
    col_129 INT,
    col_130 INT,
    col_131 INT,
    col_132 INT,
    col_133 INT,
    col_134 INT,
    col_135 INT,
    col_136 INT,
    col_137 INT,
    col_138 INT,
    col_139 INT,
    col_140 INT,
    col_141 INT,
    col_142 INT,
    col_143 INT,
    col_144 INT,
    col_145 INT,
    col_146 INT,
    col_147 INT,
    col_148 INT,
    col_149 INT,
    col_150 INT,
    col_151 INT,
    col_152 INT,
    col_153 INT,
    col_154 INT,
    col_155 BOOLEAN
);
"""

cur.execute(create_predictions_table_query)
conn.commit()

print("Table predictions has been created.")
