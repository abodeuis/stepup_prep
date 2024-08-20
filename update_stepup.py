import os
import logging 
import argparse
import pandas as pd
import requests

from cmaas_utils.logging import start_logger

from typing import List, Optional
from pydantic import BaseModel, Field, AnyUrl
from cdr_schemas.cdr_responses.legend_items import LegendItemResponse
from cmaas_utils.types import Legend, MapUnit, MapUnitType, Provenance

# region CDR Connection
class CdrConnector(BaseModel):
    system_name : str = Field(
        description="The name of the system registering with the CDR")
    system_version : str = Field(
        description="The version of the system registering with the CDR")
    token : str = Field(
        description="The token used to authenticate with the CDR")
    callback_url : AnyUrl = Field(
        description="The URL to which the CDR will send callbacks")
    callback_secret : str = Field(
        default="",
        description="The secret to use for the webhook")
    callback_username : str = Field(
        default="",
        description="The username to use for the webhook")
    callback_password : str = Field(
        default="",
        description="The password to use for the webhook")
    events : List[str] = Field(
        default_factory=list,
        description="The events to register for, leaving blank will register for all events")
    cdr_url : AnyUrl = Field(
        default="https://api.cdr.land",
        description="The URL of the CDR API")
    registration : Optional[str] = Field(
        default=None,
        description="The registration ID returned by the CDR")
    
    def register(self):
        """
        Register our system to the CDR using the app_settings
        """
        headers = {'Authorization': f'Bearer {self.token}'}
        registration = {
            "name": self.system_name,
            "version": self.system_version,
            "callback_url": str(self.callback_url),
            "webhook_secret": self.callback_secret,
            "auth_header": self.callback_username,
            "auth_token": self.callback_password,
            "events": self.events
        }
        log.info(f"Registering with CDR: [system_name : {registration['name']}, system_version : {registration['version']}, callback_url : {registration['callback_url']}")
        r = requests.post(f"{self.cdr_url}/user/me/register", json=registration, headers=headers)
        log.debug(r.text)
        r.raise_for_status()
        self.registration = r.json()["id"]
        log.info(f"Registered with CDR, registration id : {self.registration}")
        return r.json()["id"]
    
    def unregister(self):
        """
        Unregister our system from the CDR
        """
        # unregister from the CDR
        headers = {'Authorization': f"Bearer {self.token}"}
        log.info("Unregistering with CDR")
        r = requests.delete(f"{self.cdr_url}/user/me/register/{self.registration}", headers=headers)
        log.info("Unregistered with CDR")
        r.raise_for_status()
        self.registration = None

    def __str__(self) -> str:
        repr = "CdrConnector("
        repr += f"system_name='{self.system_name}', "
        repr += f"system_version='{self.system_version}', "
        repr += f"token='{self.token[:8]}...', "
        repr += f"callback_url='{self.callback_url}', "
        repr += f"callback_secret='{self.callback_secret[:8]}...', "
        repr += f"callback_username='{self.callback_username}', "
        repr += "callback_password='...', "
        repr += f"events={self.events}, "
        repr += f"cdr_url='{self.cdr_url}', "
        repr += f"registration={self.registration[:8]}..."
        repr += ")"
        return repr

    def __repr__(self) -> str:
        repr = "CdrConnector("
        repr += f"system_name='{self.system_name}', "
        repr += f"system_version='{self.system_version}', "
        repr += f"token='{self.token[:8]}...', "
        repr += f"callback_url='{self.callback_url}', "
        repr += f"callback_secret='{self.callback_secret[:8]}...', "
        repr += f"callback_username='{self.callback_username}', "
        repr += "callback_password='...', "
        repr += f"events={self.events}, "
        repr += f"cdr_url='{self.cdr_url}', "
        repr += f"registration={self.registration[:8]}..."
        repr += ")"
        return repr

    def __del__(self):
        if self.registration is not None:
            self.unregister()

def retrieve_endpoint(connection:CdrConnector, endpoint_url:str, headers:dict=None):
    if headers is None:
        headers = {'Authorization': f'Bearer {connection.token}'}
    log.debug(f"Retrieving {endpoint_url}")
    r = requests.get(endpoint_url, headers=headers)
    r.raise_for_status()
    return r.json()

def retrieve_cog_download(connection:CdrConnector, cog_id:str) -> dict:
    endpoint_url = f"{connection.cdr_url}/v1/maps/cog/{cog_id}"
    return retrieve_endpoint(connection, endpoint_url)

def retrieve_cog_legend_items(connection:CdrConnector, cog_id:str, system_id:dict=None, validated:str="false") -> List[dict]:
    # Get all legend items for a cog
    endpoint_url = f"{connection.cdr_url}/v1/features/{cog_id}/legend_items?validated={validated.lower()}"
    if system_id is not None:
        endpoint_url += f"&system_version={system_id['name']}__{system_id['version']}"
    return retrieve_endpoint(connection, endpoint_url)

def validate_cog_legend_items_response(response:List[dict]) -> List[LegendItemResponse]:
    """
    Convert the response from the cdr into a list of LegendItemResponse objects, validating the data in the process.
    """
    legend_items = []
    for item in response:
        legend_items.append(LegendItemResponse.model_validate(item))
    return legend_items

def retrieve_cog_id(connection:CdrConnector, ngmdb_id:int):
    endpoint_url = f"{connection.cdr_url}/v1/maps/ngmdb/{ngmdb_id}"
    response_data = retrieve_endpoint(connection, endpoint_url)
    return response_data['holdings']['images'][0]['cog_url']

def download_ngmdb_tif(url):
    image_response = requests.get(url)
    # Check if the request was successful
    if image_response.status_code != 200:
        log.error(f'Failed to download image. Got status code: {image_response.status_code}')
        return
    # Return image
    return image_response.content

# endregion CDR Connection

# region Update Stepup
def parse_command_line():
    """Runs Command line argument parser for pipeline. Exit program on bad arguments. Returns struct of arguments"""
    def parse_directory(path : str) -> str:
        """Command line argument parser for directory path arguments. Raises argument error if the path does not exist
           or if it is not a valid directory. Returns directory path"""
        # Check if it exists
        if not os.path.exists(path):
            msg = f'Invalid path "{path}" specified : Path does not exist\n'
            raise argparse.ArgumentTypeError(msg)
        # Check if its a directory
        if not os.path.isdir(path):
            msg = f'Invalid path "{path}" specified : Path is not a directory\n'
            raise argparse.ArgumentTypeError(msg)
        return path
    
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--inventory',
        required=True,
        help='csv inventory file')
    parser.add_argument('--data', 
        type=parse_directory,
        required=True,
        help='Should be directory with images and legends subdirectory.')  
    
    args = parser.parse_args()
    return args

def saveStepUpJson(filepath, cdr_legend:List[LegendItemResponse]):
    """Convert cdr schema legend to our internal format and save"""
    lgd = Legend(provenance=Provenance(name='labelme', version='0.0.1'))
    for feature in cdr_legend:
        map_unit = MapUnit(
            type=MapUnitType.from_str(feature.category),
            label=feature.label,
            label_confidence=feature.confidence,
            label_bbox=[feature.px_bbox[:2],feature.px_bbox[2:]],
            description=feature.description,
            abbreviation=feature.abbreviation,
            color=feature.color,
            pattern=feature.pattern
        )
        lgd.features.append(map_unit)
    with open(filepath, 'w') as fh:
        fh.write(lgd.model_dump_json())

def main(args):
    global log
    log = start_logger('UpdateStepUp', 'logs/updateStepUp.log', log_level=logging.DEBUG, console_log_level=logging.DEBUG, writemode='w')

    os.makedirs(os.path.join(args.data, 'legends'), exist_ok=True)
    os.makedirs(os.path.join(args.data, 'images'), exist_ok=True)

    log.info(f'Reading Step UP inventory file from {args.inventory}')
    inv_df = pd.read_csv(args.inventory, usecols=['proddesc','label_completed(1=yes)'], encoding = "ISO-8859-1")
    inv_df = inv_df[inv_df['label_completed(1=yes)'].notna()]
    log.info(f'Found {len(inv_df)} stepup entries')
    log.info('Initalizing CDR Connection')
    cdr_con = CdrConnector(
        system_name='ncsa_test',
        system_version='0.5',
        token='781dcc6b6814c0dbd69a6205605ea7959346d149472e5015744b63350948acb9',
        callback_url='https://criticalmaas.ncsa.illinois.edu')
    cdr_con.register()

    log.info('Processing ngmdb_id')
    bad_ids = []
    for i, iterrows in enumerate(inv_df.iterrows()):
        # Testing Debug
        # if i > 1:
        #     break
        _, row = iterrows
        ngmdb_id = row['proddesc']
        # Skip if no data is expected:
        if row['label_completed(1=yes)']  != '1':
            log.debug(f'Skipping {ngmdb_id}, label not completed')
            continue
        # Skip if data is already downloaded
        if os.path.exists(os.path.join(args.data, 'legends', f'stepup_{ngmdb_id}.json')) and os.path.exists(os.path.join(args.data, 'images', f'stepup_{ngmdb_id}.tif')):
            log.debug(f'Skipping {ngmdb_id}, already have data')
            continue

        # Retrieve the cog_url from the ngmdb_id
        try:
            cog_url = retrieve_cog_id(cdr_con, ngmdb_id)
            cog_id = cog_url.split('/')[-1].split('.')[0]
        except Exception as e:
            log.exception(f"{ngmdb_id} - Error getting cog url for ngmdb_id")
            bad_ids.append(ngmdb_id)
            continue

        log.debug(f'ngmdb_id : {ngmdb_id} got {cog_url}')
        
        # Retrieve the legend json if needed
        if not os.path.exists(os.path.join(args.data, 'legends', f'stepup_{ngmdb_id}.json')):
            response_data = retrieve_cog_legend_items(cdr_con, cog_id, system_id={'name':'labelme', 'version':'0.0.1'})
            legend_items = validate_cog_legend_items_response(response_data)
            if len(legend_items) == 0:
                log.warning(f'{ngmdb_id} - Error getting legend items')
                bad_ids.append(ngmdb_id)
                continue

            saveStepUpJson(os.path.join(args.data, 'legends', f'stepup_{ngmdb_id}.json'), legend_items)
        
        # Retrieve image if needed
        if not os.path.exists(os.path.join(args.data, 'images', f'stepup_{ngmdb_id}.tif')):
            img = download_ngmdb_tif(cog_url)

            with open(os.path.join(args.data, 'images', f'stepup_{ngmdb_id}.tif'), 'wb') as file:
                file.write(img)
        if i % 100 == 0:
            log.warning(f'processed {i}')


    if len(bad_ids) > 0:
        log.warning(f'Encountered {len(bad_ids)} bad ngmdb ids')
        log.debug(f'Bad IDS : {bad_ids}')

    cdr_con.unregister()


if __name__=='__main__':
    args = parse_command_line()
    main(args)
