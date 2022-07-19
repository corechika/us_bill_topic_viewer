import pandas as pd
import requests
import json
import base64
import sys
import os

sys.path.append('.')
from settings import API_KEY

def _get_dataset_list() -> list:
    """
    gets a list of available session datasets.

    arguments:
        None
    return:
        dataset: dict
            dictionary of session datasets
    """
    res = requests.get(f'https://api.legiscan.com/?key={API_KEY}&op=getDatasetList&state=US')
    dataset = json.loads(res.text)['datasetlist']
    return dataset

def _select_dataset_list_hash(datasets) -> list:
    """
    check a dataset hash and select datasets

    arguments:
        datasets: list
            dataset list
    return:
        list
            to read datasets list
    """
    if os.path.exists('./data/change_hash.txt'):
        # read previous hash
        with open('./data/change_hash.txt', 'r') as f:
            hash_ = f.read()
        hash_ = hash_.split('\n')
        # save hash
        with open('./data/change_hash.txt', 'w') as f:
            f.write('\n'.join([d['dataset_hash'] for d in datasets]))
        change_datasets = [d for d in datasets if d['dataset_hash'] in hash_]
        return change_datasets
    else:
        return datasets

def _collect_data(datasets) -> None:
    """
    collect a zip data

    arguments:
        datasets: list
            dataset list
    return:
        None
    """
    for d in datasets:
        res = requests.get(f'https://api.legiscan.com/?key={API_KEY}&op=getDataset&id={d["session_id"]}&access_key={d["access_key"]}')
        data = json.loads(res.text)['dataset']

        with open(f'./data/US_{d["session_id"]}_data.zip', 'wb') as f:
            f.write(base64.b64decode(data['zip']))

def collect_latest_dataset_zip() -> None:
    """
    collect datasets

    arguments:
        None
    return:
        None
        dataset zip files
    """
    datasets = _get_dataset_list()
    datasets = _select_dataset_list_hash(datasets)
    _collect_data(datasets)

def _unzip_data() -> None:
    """
    unzip latest data and remove zip files

    arguments:
        None
    return:
        None
    """
    with open('./data/change_hash.txt', 'r') as f:
        hash_ = f.read().split('\n')
    zip_name_list = os.listdir('./data/')
    pattern = regex.compile('('+'|'.join(hash_)+')', flags=regex.IGNORECASE)
    for zip_name in zip_name_list:
        if '.zip' in zip_name and regex.search(pattern, zip_name):
            tmp = "`zipinfo -1 ./data/%s | grep 'US\/[0-9]\{4\}-[0-9]\{4\}_[0-9]\{3\}th_Congress\/bill'`" % (zip_name)
            subprocess.run([f'unzip -o -d ./data ./data/{zip_name} {tmp}'], shell=True)
    # delete zip files
    subprocess.run(['rm -f ./data/US_*.zip'], shell=True)

def main():
    collect_latest_dataset_zip()

if __name__ == '__main__':
    main()