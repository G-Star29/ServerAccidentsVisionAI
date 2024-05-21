import folium
import psycopg2
import pandas as pd


# Координаты полигона Новосибирска
novosibirsk_polygon = [
    [55.0050, 82.7157],
    [54.795568, 83.046774],
    [54.940872, 83.173072],
    [55.13, 83.0020],
    [54.9924, 82.7157],
    [55.0050, 82.7157]  # Закрываем полигон, дублируя первую точку
]

# Создание карты с центром в Новосибирске
map_center = [54.9833, 82.8964]  # Координаты центра Новосибирска
mymap = folium.Map(location=map_center, zoom_start=10)

# Добавление полигона на карту
folium.Polygon(locations=novosibirsk_polygon, color='blue', fill=True, fill_color='blue', fill_opacity=0.2).add_to(mymap)

# Сохранение карты в HTML файл
mymap.save("novosibirsk_map.html")
