from dataclasses import dataclass
import requests
import pandas as pd
from tqdm import tqdm


@dataclass
class GetDataFromAPI:
    project_id: str = None
    auth_key: str = None
    headers: dict = None
    payload: dict = None
    base_url: str = None
    url: str = None
    response: requests.request = None

    def __post_init__(self):
        self.headers = dict(comp_name='WP', Authorization=f'Bearer {self.auth_key}')
        self.payload = dict()


@dataclass
class GetSubprojects(GetDataFromAPI):
    api_point = "subprojects"
    subprojects = pd.DataFrame()

    def __post_init__(self):
        super().__post_init__()
        self.base_url = f"http://45.89.26.151:3001/api/{self.api_point}"
        self.url = '?'.join([self.base_url, f"idp={self.project_id}"])
        self.__get_data()
        self.subprojects = pd.DataFrame(self.response.json().get('subprojects'))

    def __get_data(self):
        self.response = requests.request("GET", self.url, headers=self.headers, data=self.payload)
        if self.response.status_code != 200:
            raise Exception(f"\n{'*' * 100}\nError: Unable to get subprojects. "
                            f"{self.response.content}\n{'*' * 100}")


@dataclass
class GetBlocks(GetDataFromAPI):
    district_ids: list = None
    api_point = "blocks"
    blocks = pd.DataFrame()

    def __post_init__(self):
        super().__post_init__()
        self.base_url = f"http://45.89.26.151:3001/api/{self.api_point}"
        for d_id in tqdm(self.district_ids):
            self.url = '?'.join([self.base_url, f"idd={d_id}"])
            self.__get_data(ditrict_id=d_id)
            if self.response:
                if self.blocks.empty:
                    self.blocks = pd.DataFrame(self.response.json().get('blocks'))
                    continue
                self.blocks = pd.concat([self.blocks, pd.DataFrame(self.response.json().get('blocks'))],
                                        ignore_index=True)
        if self.blocks.empty:
            raise Exception(f"\n{'*' * 100}\nError: empty result blocks dataframe.\n{'*' * 100}")

    def __get_data(self, ditrict_id: str):
        self.response = requests.request("GET", self.url, headers=self.headers, data=self.payload)
        if self.response.status_code != 200:
            print(f"Error: Unable to get blocks by district for {ditrict_id}. {self.response.content}")
            self.response = None


@dataclass
class GetBlocksPolygonCenterCoords(GetDataFromAPI):
    block_ids: list = None
    api_point = "polycenter"
    polycenters = pd.DataFrame()

    def __post_init__(self):
        super().__post_init__()
        self.base_url = f"http://45.89.26.151:3001/api/{self.api_point}"
        for b_id in tqdm(self.block_ids):
            self.url = '?'.join([self.base_url, f"idb={b_id}"])
            self.__get_data(block_id=b_id)
            if self.response:
                if self.polycenters.empty:
                    self.polycenters = pd.DataFrame(self.response.json())
                    self.polycenters['latitude'] = str(round(float(self.polycenters.loc['lat', 'point']), 5))
                    self.polycenters['longitude'] = str(round(float(self.polycenters.loc['lon', 'point']), 5))
                    self.polycenters = pd.DataFrame(self.polycenters.loc['lat', :]).T.reset_index(drop=True)
                    continue
                polycenters = pd.DataFrame(self.response.json())
                polycenters['latitude'] = str(round(float(polycenters.loc['lat', 'point']), 5))
                polycenters['longitude'] = str(round(float(polycenters.loc['lon', 'point']), 5))
                polycenters = pd.DataFrame(polycenters.loc['lat', :]).T.reset_index(drop=True)
                self.polycenters = pd.concat([self.polycenters, polycenters])
                self.polycenters.reset_index(drop=True)
                if self.polycenters.empty:
                    raise Exception(f"\n{'*' * 100}\nError: empty result polygon centers dataframe.\n{'*' * 100}")

    def __get_data(self, block_id: str):
        self.response = requests.request("GET", self.url, headers=self.headers, data=self.payload)
        if self.response.status_code != 200:
            print(f"Error: Unable to get polygon coords by block id for {block_id}. {self.response.content}")
            self.response = None
