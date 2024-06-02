import os
import numpy as np
import pandas as pd
import psycopg2
from sklearn.ensemble import RandomForestClassifier, StackingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.cluster import KMeans
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, GridSearchCV, StratifiedKFold, cross_val_score
from sklearn.calibration import calibration_curve, CalibratedClassifierCV
from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss
import matplotlib.pyplot as plt
import joblib
import io
import datetime


def create_new_classifier():
    current_dir = os.path.dirname(__file__)
    kmeans_dir = os.path.abspath(os.path.join(current_dir, '..', 'kmeans'))
    scaler_dir = os.path.abspath(os.path.join(current_dir, '..', 'scaler'))
    ohe_dir = os.path.abspath(os.path.join(current_dir, '..', 'OneHotEncoder'))
    classifier_dir = os.path.abspath(os.path.join(current_dir, '..', 'classifiers'))

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
    SELECT * FROM accidentvisionai.data_for_education_table
    """

    cur.execute(select_query)

    merged_data = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    select_columns_query = "SELECT column_number, column_name FROM accidentvisionai.columns_name_table;"
    cur.execute(select_columns_query)
    columns_data = cur.fetchall()

    columns_dict = {col_num: col_name for col_num, col_name in columns_data}

    merged_df = pd.DataFrame(merged_data, columns=columns)
    merged_df.rename(columns=columns_dict, inplace=True)

    merged_df.drop(columns=['id'], inplace=True)
    merged_df.dropna(inplace=True)

    num_clusters = 25
    kmeans = KMeans(n_clusters=num_clusters, random_state=42)
    clusters = kmeans.fit_predict(merged_df[['latitude', 'longitude']])
    ohe = OneHotEncoder(sparse_output=False)
    cluster_ohe = ohe.fit_transform(clusters.reshape(-1, 1))
    print(f'[AI] Кластеризация географических данных выполнена')

    # Standardizing data
    features = merged_df.drop(['latitude', 'longitude', 'accident_occurred'], axis=1)
    labels = merged_df['accident_occurred']
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    scaled_latitude = merged_df['latitude'].values.reshape(-1, 1)
    scaled_longitude = merged_df['longitude'].values.reshape(-1, 1)

    features_final = np.hstack((features_scaled, scaled_latitude, scaled_longitude, cluster_ohe))
    print(f'[AI] Стандартизация данных выполнена')

    X_train, X_test, y_train, y_test = train_test_split(features_final, labels, test_size=0.2, random_state=42)
    print(f'[AI] Разделение данных выполнено')

    param_grid_rf = {
        'n_estimators': [100, 200, 300, 400, 500, 600],
        'max_depth': [10, 20, 30, None],
        'min_samples_split': [2, 5, 8],
        'min_samples_leaf': [1, 2, 3, 4]
    }

    param_grid_xgb = {
        'n_estimators': [100, 200, 300, 400, 500, 600],
        'max_depth': [3, 6, 10, 13, 17],
        'learning_rate': [0.01, 0.1, 0.2],
        'subsample': [0.8, 1.0]
    }

    print("[AI] Поиск оптимальных параметров для RandomForest...")
    grid_search_rf = GridSearchCV(RandomForestClassifier(random_state=42), param_grid_rf, cv=5, scoring='roc_auc',
                                  n_jobs=-1)
    grid_search_rf.fit(X_train, y_train)
    print("[AI] Поиск оптимальных параметров для RandomForest выполнено")
    print(f"[AI] Лучшие параметры для RandomForest: {grid_search_rf.best_params_}")

    print("[AI] Поиск оптимальных параметров для XGBoost...")
    xgb_clf = XGBClassifier(use_label_encoder=False, eval_metric='logloss')
    grid_search_xgb = GridSearchCV(xgb_clf, param_grid_xgb, cv=5, scoring='roc_auc', n_jobs=-1)
    grid_search_xgb.fit(X_train, y_train)
    print("[AI] Поиск оптимальных параметров для XGBoost выполнено")
    print(f"[AI] Лучшие параметры для XGBoost: {grid_search_xgb.best_params_}")

    # Лучшие модели
    best_rf = grid_search_rf.best_estimator_
    best_xgb = grid_search_xgb.best_estimator_

    print("[AI] Калибровка моделей...")
    calibrated_rf = CalibratedClassifierCV(best_rf, method='isotonic', cv=5)
    calibrated_xgb = CalibratedClassifierCV(best_xgb, method='isotonic', cv=5)
    calibrated_rf.fit(X_train, y_train)
    calibrated_xgb.fit(X_train, y_train)
    print("[AI] Калибровка моделей выполнена")

    print("[AI] Обучение Stacking...")
    estimators = [
        ('rf', calibrated_rf),
        ('xgb', calibrated_xgb)
    ]

    stacking_classifier = StackingClassifier(
        estimators=estimators,
        final_estimator=LogisticRegression()
    )

    stacking_classifier.fit(X_train, y_train)
    print("[AI] Обучение Stacking выполнена")

    probabilities = stacking_classifier.predict_proba(X_test)[:, 1]
    true_probs, predicted_probs = calibration_curve(y_test, probabilities, n_bins=10)

    plt.figure(figsize=(10, 6))
    plt.plot([0, 1], [0, 1], "k:", label="Perfectly calibrated")
    plt.plot(predicted_probs, true_probs, "s-")
    plt.xlabel("Predicted probability")
    plt.ylabel("True probability in each bin")
    plt.title("Calibration Curve")

    # Save the plot to a binary buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    calibration_curve_image = buf.read()

    auc_score = roc_auc_score(y_test, probabilities)
    print(f"[AI] AUC-ROC score: {auc_score}")

    logloss = log_loss(y_test, probabilities)
    print(f"[AI] Log Loss: {logloss}")

    brier_score = brier_score_loss(y_test, probabilities)
    print(f"[AI] Brier Score: {brier_score}")

    print("[AI] Кросс-валидация модели...")
    cv = StratifiedKFold(n_splits=5)
    cv_scores = cross_val_score(stacking_classifier, X_train, y_train, cv=cv, scoring='roc_auc')
    mean_cv_score = np.mean(cv_scores)
    std_cv_score = np.std(cv_scores)
    print("[AI] Кросс-валидация модели выполнена")

    current_date = datetime.date.today()

    insert_query = """
    INSERT INTO accidentvisionai.model_metrics_table (classifier_name, auc_roc_score, log_loss, brier_score, calibration_curve, mean_cv_score, std_cv_score, date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id
    """
    cur.execute(insert_query,
                ('StackingClassifier', auc_score, logloss, brier_score, psycopg2.Binary(calibration_curve_image),
                 mean_cv_score, std_cv_score, current_date))
    conn.commit()
    model_id = cur.fetchone()[0]

    classifier_version = f"v{model_id}"
    classifier_name = f'stacking_classifier_model_{classifier_version}.pkl'

    classifier_path = os.path.join(classifier_dir, classifier_name)
    kmeans_path = os.path.join(kmeans_dir, 'kmeans_model.pkl')
    scaler_path = os.path.join(scaler_dir, 'scaler.pkl')
    ohe_path = os.path.join(ohe_dir, 'onehotencoder.pkl')

    # Save the model with the versioned name
    joblib.dump(stacking_classifier, classifier_path)
    joblib.dump(kmeans, kmeans_path)
    joblib.dump(scaler, scaler_path)
    joblib.dump(ohe, ohe_path)

    # Update the database with the versioned classifier name
    update_query = """
    UPDATE accidentvisionai.model_metrics_table
    SET classifier_name = %s
    WHERE id = %s
    """
    cur.execute(update_query, (classifier_name, model_id))
    conn.commit()

    cur.close()
    conn.close()
