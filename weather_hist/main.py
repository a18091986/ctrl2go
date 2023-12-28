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


# TO_DO:
# 1. Объединить методы получения из базы в класс?
# 2. Добавить описание функций
# 3. Разобраться почему не по всем блокам загружаются координаты
# 4. Сделать дашборд, отработать обновление по расписанию


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
#
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

def get_subprojects(project_id: str) -> pd.DataFrame:
    subprojects = pd.DataFrame()

    base_url = "http://45.89.26.151:3001/api/subprojects"
    url = '?'.join([base_url, f"idp={project_id}"])
    headers = {
        'comp_name': 'WP',
        'Authorization': f'Bearer {auth_key}'
    }
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload)

    if response.status_code != 200:
        raise Exception(f"\n{'*' * 100}\nError: Unable to get subprojects. {response.content}\n{'*' * 100}")

    subprojects = pd.DataFrame(response.json().get('subprojects'))
    return subprojects


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

    if result_df.empty:
        raise Exception(f"\n{'*' * 100}\nError: empty result blocks dataframe.\n{'*' * 100}")
    return result_df


def get_coords_of_polygon_center_of_blocks(block_ids: list[str, ...]) -> pd.DataFrame:
    result_df = pd.DataFrame()

    base_url = "http://45.89.26.151:3001/api/polycenter"
    headers = {
        'comp_name': 'WP',
        'Authorization': f'Bearer {auth_key}'
    }
    payload = {}
    for b_id in block_ids:
        url = '?'.join([base_url, f"idb={b_id}"])
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code != 200:
            print(f"Error: Unable to get polygon coords by block id for {b_id}. {response.content}")
            continue
        if result_df.empty:
            result_df = pd.DataFrame(response.json())
            result_df['latitude'] = str(round(float(result_df.loc['lat', 'point']), 5))
            result_df['longitude'] = str(round(float(result_df.loc['lon', 'point']), 5))
            result_df = pd.DataFrame(result_df.loc['lat', :]).T.reset_index(drop=True)
            continue

        current_df = pd.DataFrame(response.json())
        current_df['latitude'] = str(round(float(current_df.loc['lat', 'point']), 5))
        current_df['longitude'] = str(round(float(current_df.loc['lon', 'point']), 5))
        current_df = pd.DataFrame(current_df.loc['lat', :]).T.reset_index(drop=True)
        result_df = pd.concat([result_df, current_df])

    return result_df.reset_index(drop=True)


def get_history_weather(datamode: str = 'bilinear', src_id: int = 2,
                        geo_points: Tuple[Tuple[str, str], ...] = (("28.632854", "77.219721"),),
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

    return weather_hist

    # if out_format == 'xlsx':
    #     weather_hist.to_excel(Path(dir_to_save, f"weather_{datetime.now().strftime('%Y%m%d__%H%M%S')}.xlsx"))
    #     exit()
    #
    # elif out_format == 'sql':
    #     pass
    #     exit()
    #
    # weather_hist.to_csv(Path(dir_to_save, f"weather_{datetime.now().strftime('%Y%m%d__%H%M%S')}.csv"))


if __name__ == '__main__':
    dir_to_save = Path("out_data")
    dir_to_save.mkdir(exist_ok=True)
    ID_PROJECT = '12'
    START_DATE = '1990-01-01'
    END_DATE = '1990-01-04'

    subprojects_df = get_subprojects(project_id=ID_PROJECT)
    subprojects_df = subprojects_df[['id_subproject', 'id_atd']].rename(columns={'id_atd': "distrCode"})
    district_ids_list = subprojects_df['distrCode'].to_list()  # == atd_ids

    blocks_df = get_blocks_by_district(district_ids_list)

    subprojects_districts_blocks_df = subprojects_df.merge(blocks_df, on='distrCode', how='left')

    blocks_list = subprojects_districts_blocks_df['BlockCode'].to_list()

    blocks_with_coords_df = get_coords_of_polygon_center_of_blocks(blocks_list)[['name', 'latitude', 'longitude']]

    geo_df = subprojects_districts_blocks_df.merge(blocks_with_coords_df,
                                                   left_on='Blockname', right_on='name', how='left')

    geo_df = geo_df[[x for x in geo_df.columns if x not in ['BlockAltNames', 'mrdcode', 'distrCode',
                                                            'stateCode', 'name', 'BlockCode']]]
    geo_df.loc[:, ['latitude', 'longitude']] = geo_df.loc[:, ['latitude', 'longitude']].astype(float)

    geo_points = tuple((x[0], x[1]) for x in
                       geo_df.loc[:, ['latitude', 'longitude']].values.tolist()
                       if not np.isnan(float(x[0])))

    weather_history_df = get_history_weather(geo_points=geo_points,
                                             start_date=START_DATE, end_date=END_DATE,
                                             out_format='csv')

    final = weather_history_df.merge(geo_df, on='latitude', how='left') \
        .drop(columns=['longitude_y']) \
        .rename(columns={'longitude_x': 'Longitude',
                         'latitude': 'Latitude',
                         'date': 'Date',
                         'meanTemperature': 'Average temperature',
                         'minimalTemperature': 'Minimum temperature',
                         'maximalTemperature': 'Maximum temperature',
                         'precipitation': 'Total precipitation',
                         'relativeHumidity': 'Relative humidity',
                         'id_subproject': 'Subproject',
                         'distr': 'District',
                         'Blockname': 'Block',
                         'state': 'State'})

    final = final[['Latitude', 'Longitude', 'Subproject', 'State', 'District', 'Block', 'Date',
                   'Average temperature', 'Maximum temperature', 'Minimum temperature',
                   'Total precipitation', 'Relative humidity']]

    final.to_csv(Path(dir_to_save, f"weather_hist_{datetime.now().strftime('%Y%m%d__%H%M%S')}.csv"))
    final.to_excel(Path(dir_to_save, f"weather_hist_{datetime.now().strftime('%Y%m%d__%H%M%S')}.xlsx"))
    # final = final.set_index(['Latitude', 'Longitude'])
    # print(final.to_string())
