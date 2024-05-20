import requests
import json
import pandas as pd

# URL для отправки POST запроса
url = 'http://stat.gibdd.ru/map/getDTPCardData'  # замените на нужный URL

# Данные для отправки
data = {
    "data": "{\"date\":[\"MONTHS:3.2024\"],\"ParReg\":\"50\",\"order\":{\"type\":\"1\",\"fieldName\":\"dat\"},\"reg\":\"50401\",\"ind\":\"1\",\"st\":\"1\",\"en\":\"16\"}"
}

# Отправка POST запроса и получение ответа
response = requests.post(url, json=data)

# Проверка успешности запроса
if response.status_code == 200:
    # Имя файла для сохранения данных
    filename = 'response.json'

    try:
        # Сохранение данных в JSON файл
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(response.json(), f, ensure_ascii=False, indent=4)
        print('Data saved successfully.')
        with open('response.json', 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        data = json.loads(json_data["data"])
        accidents = []
        for entry in data["tab"]:
            accident = {
                "id": entry["KartId"],
                "latitude": entry["infoDtp"]["COORD_W"],
                "longitude": entry["infoDtp"]["COORD_L"],
                "weather": entry["infoDtp"]["s_pog"][0],
                "datetime": f"{entry['date']} {entry['Time']}",
                "ligth": entry["infoDtp"]["osv"],
                "road_conditions": "; ".join(entry["infoDtp"]["ndu"]),
                "nearby": "; ".join(entry["infoDtp"]["OBJ_DTP"])
            }
            accidents.append(accident)

        # Создание DataFrame из списка
        df = pd.DataFrame(accidents)

        # Сохранение данных в новый CSV файл
        output_csv_file_path = 'parsed_output.csv'
        df.to_csv(output_csv_file_path, index=False)

        print("Data parsed and saved successfully to parsed_output.csv")
    except Exception as e:
        print(f'Error saving data: {str(e)}')


else:
    print(f'Failed to send POST request. Status code: {response.status_code}')
    print(f'Response text: {response.text}')

