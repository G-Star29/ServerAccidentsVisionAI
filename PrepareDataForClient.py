import numpy as np
from geopy.distance import great_circle
from sklearn.cluster import DBSCAN

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
            if np.isnan(lat) or np.isnan(lon):
                raise ValueError("NaN value found")
            coords.append([lat, lon])
            probs.append(value)
        except (ValueError, AttributeError) as e:
            print(f"Ошибка при обработке координат {key}: {e}")
            continue

    coords = np.array(coords)
    probs = np.array(probs)

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

        # Найти точку, ближайшую ко всем остальным точкам в кластере
        distances_sum = np.sum([[great_circle(tuple(cluster_coords[j]), tuple(cluster_coords[i])).meters
                                 for j in range(len(cluster_coords))]
                                for i in range(len(cluster_coords))], axis=1)
        center_idx = np.argmin(distances_sum)
        center_coord = cluster_coords[center_idx]
        mean_prob = np.mean(cluster_probs)

        clusters[tuple(center_coord)] = mean_prob

    return clusters
