from datetime import datetime

import numpy as np
import pandas as pd
from pathlib import Path
import dotenv
import requests
import json
from typing import Tuple

env = dotenv.dotenv_values('../.env')
auth_key = env.get("AUTH_TOKEN")


def get_list_of_districts_ids(project_title: str = 'Rabi 2022-23') -> list[str, ...]:
    url = f"http://45.89.26.151:3001/api/projectsJSON?namep={project_title}"
    headers = {
        'comp_name': 'WP',
        'Authorization': f'Bearer {auth_key}'
    }
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    return [x.get('code_devision') for x in response.json().get('projects')[0].get('atd_units')]
    # print(json.dumps(response.json(), indent=3))


# district_ids = get_list_of_districts_ids()


def get_blocks_by_district(district_ids: list[str, ...]) -> pd.DataFrame:
    result_df = pd.DataFrame()
    base_url = f"http://45.89.26.151:3001/api/blocks"
    headers = {
        'comp_name': 'WP',
        'Authorization': f'Bearer {auth_key}'
    }
    payload = {}
    for d_id in district_ids:
        url = '?'.join([base_url, f"idd={d_id}"])
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code != 200:
            print(f"Error: Unable to get blocks by district for {d_id}. {response.content}")
            continue
        if result_df.empty:
            result_df = pd.DataFrame(response.json().get('blocks'))
            continue
        result_df = pd.concat([result_df, pd.DataFrame(response.json().get('blocks'))], ignore_index=True)

    return result_df


# print(get_blocks_by_district(district_ids).shape)


def get_coords_of_polygon_center_of_block():
    url = "http://45.89.26.151:3001/api/polycenter?idb=4045"
    headers = {
        'comp_name': 'WP',
        'Authorization': f'Bearer {auth_key}'
    }
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    print(response.url)
    print(pd.DataFrame(response.json()))


# def get_gp_by_district():
#     url = "http://45.89.26.151:3001/api/grams?idd=437"
#     headers = {
#         'comp_name': 'WP',
#         'Authorization': f'Bearer {auth_key}'
#     }
#     payload = {}
#     response = requests.request("GET", url, headers=headers, data=payload)
#     # print(json.dumps(response.json(), indent=3))
#     print(pd.DataFrame(response.json().get('grams')))

# def get_coords_of_polygon_center_of_gp():
#     url = "http://45.89.26.151:3001/api/grams?idd=437"
#     headers = {
#         'comp_name': 'WP',
#         'Authorization': f'Bearer {auth_key}'
#     }
#     payload = {}
#     response = requests.request("GET", url, headers=headers, data=payload)
#     # print(json.dumps(response.json(), indent=3))
#     print(pd.DataFrame(response.json().get('grams')))


def get_history_weather(datamode: str = 'bilinear', src_id: int = 2,
                        geo_points: Tuple[Tuple[float | int, float | int], ...] = ((28.632854, 77.219721),),
                        start_date: str = "1990-01-01", end_date: str = "1990-01-03",
                        out_format: str = 'csv') -> pd.DataFrame | None:
    """
    Parameters
    ----------
    datamode - интерполяция
    src_id - источник данных
    geo_points - набор кортежей широта-долгота географических точек
    start_date - начальная дата
    end_date - конечная дата
    out_format - выходной формат данных: csv, xlsx или загрузка в БД

    Returns
    -------

    """
    weather_hist = pd.DataFrame()
    dir_to_save = Path("out_data")
    dir_to_save.mkdir(exist_ok=True)

    base_url = "http://212.41.22.36:8084/meteo/api/v1/webportal/ptDailyMeteoData"
    parameters = f"srcId={src_id}&dataMode={datamode}&startDate={start_date}&endDate={end_date}"
    url = '?'.join([base_url, parameters])

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {auth_key}'
    }

    # формируем тело запроса из набора координат
    geo_points_dict = list(map(lambda x: dict(latitude=x[0], longitude=x[1]), geo_points))
    geo_points_json = json.dumps(geo_points_dict)

    response = requests.request("POST", url, headers=headers, data=geo_points_json)

    if response.status_code != 200:
        print(f"Error: Unable to fetch meteo data. {response.content}")
        return weather_hist

    weather_hist = pd.DataFrame.from_records(response.json())
    weather_hist = weather_hist.loc[:, ~weather_hist.columns.isin(['sourceId', 'isInterpolated'])]

    if out_format == 'xlsx':
        weather_hist.to_excel(Path(dir_to_save, f"weather_{datetime.now().strftime('%Y%m%d__%H%M%S')}.xlsx"))
        exit()

    elif out_format == 'sql':
        pass
        exit()

    weather_hist.to_csv(Path(dir_to_save, f"weather_{datetime.now().strftime('%Y%m%d__%H%M%S')}.csv"))
    return weather_hist


# if __name__ == '__main__':
# get_districts()
# get_blocks_by_district()
# get_gp_by_district()
get_coords_of_polygon_center_of_block()
# geo_points_test = (
#     (21.883583, 77.205752),
#     (23.883583, 80.205752),
#     (17.883583, 74.205752)
# )

# geo_points_test = tuple((x, y) for x in np.arange(20, 21, 0.25) for y in np.arange(77, 78, 0.25))
# start_date = '1990-01-01'
# end_date = '1990-01-03'
#
# weather = get_history_weather(geo_points=geo_points_test, start_date=start_date, end_date=end_date,
#                               out_format='csv')

# weather = weather.set_index(['latitude', 'longitude'])
# print(weather.head(10).to_string())
