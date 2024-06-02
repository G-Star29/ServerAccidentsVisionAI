import base64

import numpy as np
import pandas as pd
import psycopg2
from pydantic import BaseModel

class ModelData(BaseModel):
    classifier_name: str
    auc_roc_score: float
    log_loss: float
    calibration_curve: str
    mean_cv_score: float
    std_cv_score: float
    date: str

def get_model_data_from_db():
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

    select_query = """
    SELECT classifier_name, auc_roc_score, log_loss, calibration_curve, mean_cv_score, std_cv_score, date
    FROM accidentsvisionai.accidentvisionai.model_metrics_table
    """

    cur.execute(select_query)
    result_of_cur = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    data_df = pd.DataFrame(result_of_cur, columns=columns)

    # Преобразование данных в список словарей
    data_df['calibration_curve'] = data_df['calibration_curve'].apply(lambda x: base64.b64encode(x).decode('utf-8'))
    # Преобразование даты в строку
    data_df['date'] = data_df['date'].astype(str)

    data_df = data_df.replace({np.nan: None})

    data_df['mean_cv_score'] = data_df['mean_cv_score'].replace({np.nan: 0.0, None: 0.0})
    data_df['std_cv_score'] = data_df['std_cv_score'].replace({np.nan: 0.0, None: 0.0})

    result = data_df.to_dict(orient='records')

    # Закрытие соединения
    cur.close()
    conn.close()

    return result


