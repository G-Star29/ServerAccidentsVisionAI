begin;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE,
    boundary GEOMETRY
);

-- Добавление записи для Новосибирска (если еще не добавлено)
INSERT INTO cities (name, boundary)
VALUES ('Новосибирск', ST_GeomFromText('POLYGON((82.7157 55.0050, 83.046774 54.795568, 83.173072 54.940872, 83.0020 55.13, 82.7157 54.9924, 82.7157 55.0050))', 4326))
ON CONFLICT (name) DO NOTHING;


-- Удаление всех записей из таблицы coords_and_nearby, которые не входят в границы Новосибирска
DELETE FROM accidentvisionai.coords_and_nearby
WHERE NOT ST_Contains(
    (SELECT boundary FROM cities WHERE name = 'Новосибирск'),
    ST_SetSRID(ST_MakePoint(col_3, col_2), 4326)
);

-- Если все прошло успешно, фиксируем транзакцию
COMMIT;

