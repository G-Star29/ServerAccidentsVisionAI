import psycopg2


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

create_predictions_results_table_query = create_table_query = """
DROP TABLE IF EXISTS accidentvisionai.predictions_results;

CREATE TABLE accidentvisionai.predictions_results (
    prediction_id SERIAL PRIMARY KEY,
    coords_id INTEGER REFERENCES accidentvisionai.coords_and_nearby(col_1) ON DELETE CASCADE, 
    indications_id INTEGER REFERENCES accidentvisionai.indications_table(id),
    prediction_date DATE,
    prediction_time TIME,
    prediction_value FLOAT
);
"""

cur.execute(create_predictions_results_table_query)
conn.commit()

print("Table predictions_results has been created.")