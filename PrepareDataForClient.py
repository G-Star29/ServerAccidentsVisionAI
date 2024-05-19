import numpy as np
import math
from sklearn.cluster import DBSCAN


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # Радиус Земли в метрах
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


def PrepareDataForClient(data):
    coords = []
    probs = []

    for key, value in data.items():
        try:
            if isinstance(key, tuple):
                lat, lon = key
                lat, lon = float(lat), float(lon)
            else:
                lat, lon = map(float, key.split(","))
            if math.isnan(lat) or math.isnan(lon):
                raise ValueError("NaN value found")
            coords.append([lat, lon])
            probs.append(value)
        except (ValueError, AttributeError) as e:
            print(f"Ошибка при обработке координат {key}: {e}")
            continue

    coords = np.array(coords)
    probs = np.array(probs)

    # Убедиться, что нет NaN в координатах
    if np.isnan(coords).any():
        raise ValueError("Found NaN in coordinates array")

    kms_per_radian = 6371.0088
    epsilon = 50 / 1000.0 / kms_per_radian

    db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))

    labels = db.labels_
    unique_labels = set(labels)

    clusters = {}

    for label in unique_labels:
        cluster_coords = coords[labels == label]
        cluster_probs = probs[labels == label]

        mean_lat = np.mean(cluster_coords[:, 0])
        mean_lon = np.mean(cluster_coords[:, 1])
        mean_prob = np.mean(cluster_probs)

        clusters[(mean_lat, mean_lon)] = mean_prob

    return clusters