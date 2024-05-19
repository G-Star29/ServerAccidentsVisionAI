import pandas as pd
import psycopg2
import joblib
import numpy as np
from datetime import datetime

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
CREATE TABLE IF NOT EXISTS accidentvisionai.predictions_results (
    prediction_id SERIAL PRIMARY KEY,
    coords_id INTEGER REFERENCES accidentvisionai.coords_and_nearby(col_1),
    prediction_date DATE,
    prediction_time TIME,
    prediction_value FLOAT
);
"""

cur.execute(create_predictions_results_table_query)
conn.commit()

print("Table predictions_results has been created.")