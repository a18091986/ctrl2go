import requests
import json

#  Получение данных о погоде
meteo_api_url = "http://192.168.0.10:8084/meteo/api/v1/webportal/interpolPointParamsMap?endDate=03-01-2019&startDate=01-01-2019"
response = requests.get(meteo_api_url)

if response.status_code != 200:
    print(f"Error: Unable to fetch meteo data. {response.content}")
    exit()

meteo_data = response.json()

#  Обработка данных
datahub_data = {
    "data": []
}
for item in meteo_data.get("data", []):
    transformed_item = {
        "project": item.get("project", 1),
        "subproject": item.get("subproject", 2),
        "latitude": item.get("latitude"),
        "longitude": item.get("longitude"),
        "id_state": item.get("id_state"),
        "id_district": item.get("id_district"),
        "id_block": item.get("id_block"),
        "id_gp": item.get("id_gp"),
        "division_level": item.get("division_level"),
        "date": item.get("date"),
        "mean_temp": item.get("mean_temp"),
        "max_temp": item.get("max_temp"),
        "min_temp": item.get("min_temp"),
        "prec": item.get("prec"),
        "mean_rh": item.get("mean_rh"),
        "max_rh": item.get("max_rh"),
        "min_rh": item.get("min_rh"),
        "rad": item.get("rad")
    }
    datahub_data["data"].append(transformed_item)

# . Отправка данных в datahub
datahub_api_url = "http://212.41.22.38:8094/api/v1/weather-data/daily"
headers = {
    "Content-Type": "application/json"
}

response = requests.post(datahub_api_url, data=json.dumps(datahub_data), headers=headers)

if response.status_code == 200:
    print("Success: Records created in datahub")
else:
    print(f"Error: Unable to create records in datahub. {response.content}")
