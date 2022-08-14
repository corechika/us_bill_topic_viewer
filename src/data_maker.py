import base64
import json
import os
import subprocess
import sys
from collections import defaultdict

import pandas as pd
import regex
import requests
import spacy
from gensim.corpora.dictionary import Dictionary
from gensim.models.ldamodel import LdaModel
from gensim.test.utils import datapath

nlp = spacy.load("en_core_web_sm")

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
            hash_ = f.read().split('\n')
        # save hash
        with open('./data/change_hash.txt', 'w') as f:
            f.write('\n'.join([d['dataset_hash'] for d in datasets]))
        change_datasets = [d for d in datasets if d['dataset_hash'] not in hash_]
        return change_datasets
    else:
        with open('./data/change_hash.txt', 'w') as f:
            f.write('\n'.join([d['dataset_hash'] for d in datasets]))
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
        print(f'\rcollecting {d["session_title"]}')
        res = requests.get(f'https://api.legiscan.com/?key={API_KEY}&op=getDataset&id={d["session_id"]}&access_key={d["access_key"]}')
        data = json.loads(res.text)['dataset']

        with open(f'./data/US_{d["session_id"]}_data_d{d["dataset_hash"]}.zip', 'wb') as f:
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
    if datasets == []:
        print('No change files.')
        sys.exit()
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

def _collect_json_2_df() -> pd.DataFrame:
    """
    collecting bills

    arguments:
        None
    return:
        bill_df: pd.DataFrame
            bill data frame
    """
    bill_df = pd.DataFrame()
    congress_list = subprocess.run(["ls ./data/US | grep '[0-9]\{4\}-[0-9]\{4\}_[0-9]\{3\}th_Congress'"], shell=True, capture_output=True).stdout.decode().split()
    for congress in congress_list:
        print(f'\rprocessing {congress}', end='')
        bill_files = os.listdir(f'./data/US/{congress}/bill')
        for file in bill_files:
            if '.json' in file:
                with open(f'./data/US/{congress}/bill/{file}', 'r') as f:
                    data = json.load(f)
                bill_df = pd.concat([bill_df, pd.DataFrame(data).T])
    print()
    return bill_df

def _extract_word(text) -> list:
    """
    extract the word to need

    arguments:
        text: str
            congress's title and description text
    return:
        token_list: list
            token's list
    """
    doc = nlp(text)
    token_list = [token for token in doc if token.pos_ not in ['ADP', 'AUX', 'DET', 'NUM', 'PUNCT', 'SYM', 'SPACE']]

    return token_list

def extract_corpus() -> pd.DataFrame:
    """
    to extract Topic model's corpus

    arguments:
        None
    return:
        corpus: pd.DataFrame
            corpus list in us congress
    """
    us_congress = pd.read_csv('./data/US_congress.csv')
    us_congress = us_congress[(us_congress['description'].notnull()) & (us_congress['title'].notnull())]
    
    title_desc = us_congress['title'] + '. ' + us_congress['description']
    us_congress['corpus'] = title_desc.apply(lambda x: _extract_word(x))

    return us_congress

def create_bill_list_4_sponsor(us_congress) -> None:
    """
    create bill list for each sponsor
    ex) {9403: [202753, 203421, 196085]}

    arguments:
        us_congress: pd.DataFrame
            bill data
    return:
        None
    """
    sponsors = us_congress['sponsors'].apply(lambda x: eval(x))
    bill_sponsor_dict = defaultdict(list)

    for bill_id, sponsor in zip(us_congress['bill_id'], sponsors):
        for s in sponsor:
            if s != []:
                bill_sponsor_dict[s['people_id']].append(bill_id)
    
    with open('./data/sponsor_bill_dict.json', 'w') as f:
        json.dump(bill_sponsor_dict, f, indent=2, ensure_ascii=False)

def _create_dictonary(corpus_text) -> Dictionary:
    """
    create gensim dictionary from bill text

    arguments:
        corpus_text: list
            list of bill text
    return:
        bill_dictionary: Dictionary
    """
    bill_dictionary = Dictionary(corpus_text)
    return bill_dictionary

def _create_corpus(bill_dictionary, corpus_text) -> list:
    """
    translate text to gensim corpus

    arguments:
        bill_dictionary: Dictionary
            created gensim dictionary
        corpus_text: list
            list of bill text
    return:
        bill_corpus: list
            gensim corpus
    """
    bill_corpus = [bill_dictionary.doc2bow(text) for text in corpus_text]
    return bill_corpus

def _train_lda(bill_corpus) -> LdaModel:
    """
    training LDA model from gensim corpus

    arguments:
        bill_corpus: list
            created gensim corpus
    return:
        lda: LdaModel
    """
    lda = LdaModel(bill_corpus, num_topics=5)
    # Save model to disk.
    temp_file = datapath("./data/model")
    lda.save(temp_file)

    return lda

def create_lda_model(bill_text) -> LdaModel:
    """
    created LDA model from bill text

    arguments:
        bill_text: pd.Series
            congress bill text
    return:
        lda: LdaModel
    """
    if type(bill_text[0]) == list:
        corpus = bill_text.tolist()
    else:
        corpus = bill_text.apply(lambda x: eval(x)).tolist()
    bill_dictionary = _create_dictonary(corpus)
    train_corpus = _create_corpus(bill_dictionary, corpus)
    lda_model = _train_lda(train_corpus)
    
    return lda_model

def main():
    # collect data
    collect_latest_dataset_zip()
    _unzip_data()
    us_congress = _collect_json_2_df()

    if os.path.exists('./data/US_congress.csv'):
        tmp = pd.read_csv('./data/US_congress.csv')
        us_congress = pd.concat([us_congress, tmp]).drop_duplicates(subset=['bill_id'])
    us_congress.to_csv('./data/US_congress.csv', index=False)

    # extract corpus
    us_congress = extract_corpus()

    # creation of bill list for each sponsor
    create_bill_list_4_sponsor(us_congress)

    # train Topic model
    lda_model = create_lda_model(us_congress['corpus'])

if __name__ == '__main__':
    main()
