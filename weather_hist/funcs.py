from datetime import datetime
import numpy as np
import pandas as pd

from pathlib import Path
import requests
import json
from typing import Tuple

from weather_hist.models import GetSubprojects, GetBlocks, GetBlocksPolygonCenterCoords


def get_history_weather_by_coords(auth_key: str, datamode: str = 'bilinear', src_id: int = 2,
                                  geo_points: Tuple[Tuple[str, str], ...] = (("28.632854", "77.219721"),),
                                  start_date: str = "1990-01-01", end_date: str = "1990-01-03", ) \
        -> (pd.DataFrame | None):
    """
    Parameters
    ----------
    auth_key - токен api
    datamode - интерполяция
    src_id - источник данных
    geo_points - набор кортежей широта-долгота географических точек
    start_date - начальная дата
    end_date - конечная дата

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


def get_weather_history_with_geo(
        project_id: str,
        auth_key: str,
        start_date: str = "1990-01-01",
        end_date: str = "1990-01-03",
        out_format: str = 'csv',
        path_to_save: Path = Path('out_data')) -> pd.DataFrame | None:
    """

    Parameters
    ----------
    project_id - номер проекта
    auth_key - токен api
    start_date - начальная дата
    end_date - конечная дата
    out_format - выходной формат данных: csv, xlsx или загрузка в БД

    Returns
    -------

    """

    # get subprojects and corresponding districts ids by project id
    print("GET SUBPROJECT")
    subprojects = GetSubprojects(project_id=project_id, auth_key=auth_key).subprojects
    subprojects = subprojects[['id_subproject', 'id_atd']].rename(columns={'id_atd': "distrCode"})
    district_ids_list = subprojects['distrCode'].to_list()  # == atd_ids

    # get blocks by districts
    print("GET BLOCKS")
    blocks = GetBlocks(project_id=project_id, auth_key=auth_key, district_ids=district_ids_list).blocks
    subprojects_districts_blocks = subprojects.merge(blocks, on='distrCode', how='left')
    blocks_list = subprojects_districts_blocks['BlockCode'].to_list()

    # get blocks coords
    print("GET COORDS")
    coords = GetBlocksPolygonCenterCoords(project_id=project_id, auth_key=auth_key, block_ids=blocks_list) \
        .polycenters
    blocks_with_coords = coords[['name', 'latitude', 'longitude']]

    geo_df = subprojects_districts_blocks.merge(blocks_with_coords, left_on='Blockname', right_on='name', how='left')
    geo_df = geo_df[[x for x in geo_df.columns if x not in ['BlockAltNames', 'mrdcode', 'distrCode',
                                                            'stateCode', 'name', 'BlockCode']]]
    geo_df.loc[:, ['latitude', 'longitude']] = geo_df.loc[:, ['latitude', 'longitude']].astype(float)
    geo_points = tuple((x[0], x[1]) for x in
                       geo_df.loc[:, ['latitude', 'longitude']].values.tolist()
                       if not np.isnan(float(x[0])))

    # get weather history
    print("GET WEATHER")
    weather_history_df = get_history_weather_by_coords(auth_key=auth_key, geo_points=geo_points,
                                                       start_date=start_date, end_date=end_date)

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

    if out_format == 'csv':
        final.to_csv(Path(path_to_save, f"weather_hist_{datetime.now().strftime('%Y%m%d__%H%M%S')}.csv"),
                     index=False)
    if out_format == 'xlsx':
        final.to_excel(Path(path_to_save, f"weather_hist_{datetime.now().strftime('%Y%m%d__%H%M%S')}.xlsx"),
                       index=False)

    return final
