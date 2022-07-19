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

def main():
    collect_latest_dataset_zip()

if __name__ == '__main__':
    main()