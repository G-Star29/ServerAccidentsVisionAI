import pandas as pd
import psycopg2
import joblib
import numpy as np
from datetime import datetime


def make_prediction_from_latest_model(for_current_state=True, dataFrame_for_prediction=None):
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

    select_model_query = """
        SELECT * FROM accidentvisionai.model_metrics_table ORDER BY id DESC LIMIT 1;
        """

    cur.execute(select_model_query)
    model_selected = cur.fetchall()
    print(model_selected[0][1])

    global prediction_date, prediction_time
    loaded_model = joblib.load(f'classifiers/{model_selected[0][1]}')
    loaded_kmeans = joblib.load('kmeans/kmeans_model.pkl')
    loaded_scaler = joblib.load('scaler/scaler.pkl')
    loaded_ohe = joblib.load('OneHotEncoder/onehotencoder.pkl')

    print("[PREDICTION] Загружена последняя модель")

    if for_current_state:
        # Извлечение данных из таблиц coords_and_nearby и data_for_prediction_table, объединяя их по ключу coords_and_nearby
        select_query = """
        SELECT
            c.col_1, c.col_2, c.col_3, p.col_5, p.col_6, p.col_7, p.col_8, p.col_9, p.col_10,
            p.col_11, p.col_12, p.col_13, p.col_14, p.col_15, p.col_16, p.col_17, p.col_18, p.col_20, p.col_21,
            p.col_22, p.col_23, p.col_24, p.col_25, c.col_26, c.col_27, c.col_28, c.col_29, c.col_30, c.col_31, c.col_32, c.col_33,
            c.col_34, c.col_35, c.col_36, c.col_37, c.col_38, c.col_39, c.col_40, c.col_41, c.col_42, c.col_43,
            c.col_44, c.col_45, c.col_46, c.col_47, c.col_48, c.col_49, c.col_50, c.col_51, c.col_52, c.col_53,
            c.col_54, c.col_55, c.col_56, c.col_57, c.col_58, c.col_59, c.col_60, c.col_61, c.col_62, c.col_63,
            c.col_64, c.col_65, c.col_66, c.col_67, c.col_68, c.col_69, c.col_70, c.col_71, c.col_72, c.col_73,
            c.col_74, c.col_75, c.col_76, c.col_77, c.col_78, c.col_79, p.col_80, p.col_81, p.col_82, p.col_83, p.col_84, p.col_85,
            p.col_86, p.col_87, p.col_88, p.col_89, p.col_90, p.col_91, p.col_92, p.col_93, p.col_94, p.col_95,
            p.col_96, p.col_97, p.col_98, p.col_99, p.col_100, p.col_101, p.col_102, p.col_103, p.col_104, p.col_105,
            p.col_106, p.col_107, p.col_108, p.col_109, p.col_110, p.col_111, p.col_112, p.col_113, p.col_114, p.col_115,
            p.col_116, p.col_117, p.col_118, p.col_119, p.col_120, p.col_121, p.col_122, p.col_123, p.col_124, p.col_125,
            p.col_126, p.col_127, p.col_128, p.col_129, p.col_130, p.col_131, p.col_132, p.col_133, p.col_134, p.col_135,
            p.col_136, p.col_137, p.col_138, p.col_139, p.col_140, p.col_141, p.col_142, p.col_143, p.col_144, p.col_145,
            p.col_146, p.col_147, p.col_148, p.col_149, p.col_150, p.col_151, p.col_152, p.col_153, p.col_154, p.col_155
        FROM accidentvisionai.data_for_prediction_table p
        JOIN accidentvisionai.coords_and_nearby c
        ON p.coords_and_nearby = c.col_1;
        """

        cur.execute(select_query)

        merged_data = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        merged_df = pd.DataFrame(merged_data, columns=columns)

        merged_df_for_results = merged_df.copy()

    else:
        merged_df = dataFrame_for_prediction

    select_columns_query = "SELECT column_number, column_name FROM accidentvisionai.columns_name_table;"
    cur.execute(select_columns_query)
    columns_data = cur.fetchall()

    columns_dict = {col_num: col_name for col_num, col_name in columns_data}

    merged_df.rename(columns=columns_dict, inplace=True)
    # Извлечение признаков для предсказания
    X = merged_df

    if for_current_state:
        X.drop('id', axis=1, inplace=True)
    X.dropna(inplace=True)

    if X.shape[0] == 0:
        print("[PREDICTION] Ошибка! Кластеризация данных невозможна")
        return

    clusters = loaded_kmeans.predict(X[['latitude', 'longitude']])
    cluster_ohe = loaded_ohe.transform(clusters.reshape(-1, 1))
    features = X.drop(['latitude', 'longitude'], axis=1)
    features_scaled = loaded_scaler.transform(features)
    scaled_latitude = X['latitude'].values.reshape(-1, 1)
    scaled_longitude = X['longitude'].values.reshape(-1, 1)
    features_final = np.hstack((features_scaled, scaled_latitude, scaled_longitude, cluster_ohe))

    # Выполнение предсказаний
    predictions = loaded_model.predict_proba(features_final)
    print("[PREDICTION] Прогноз cформирован")

    if for_current_state:
        insert_query = """
        INSERT INTO accidentvisionai.predictions_results (coords_id, prediction_date, prediction_time, prediction_value)
        VALUES (%s, %s, %s, %s)
        """

        for idx, prediction in enumerate(predictions):
            coords_id = int(merged_df_for_results.iloc[idx]['col_1'])  # Преобразование в стандартный тип int
            prediction_date = datetime.now().date()
            prediction_time = datetime.now().time()
            prediction_value = float(prediction[1])  # Преобразование в стандартный тип float
            cur.execute(insert_query, (coords_id, prediction_date, prediction_time, prediction_value))

        print(f"[PREDICTION] Прогноз загружен в БД {prediction_date} {prediction_time}")

        conn.commit()
        cur.close()
        conn.close()

    else:
        formatted_predictions = {}
        for idx, prediction in enumerate(predictions):
            latitude = X.iloc[idx]['latitude']
            longitude = X.iloc[idx]['longitude']
            prediction_value = prediction[1]
            formatted_predictions[(latitude, longitude)] = prediction_value

        print(f"[PREDICTION] Прогноз отправлен")
        return formatted_predictions
