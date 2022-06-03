"""
Doc String
This 
"""

from pathlib import Path
import requests
import pandas as pd
import warnings
import time
import os
from datetime import datetime
from zhon.hanzi import punctuation
from google.cloud import bigquery

warnings.filterwarnings('ignore')


# Environments Settings
project_dir =  Path(os.getcwd()).parents[0]
job_dir = Path(os.getcwd())
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(project_dir,'config/plants-bigquery-key.json')



def main():
    # Webcrawler
    keyword = '果苗'

    item_list = []
    for s in ['0', '100', '200', '300', '400']:
        url = f"https://shopee.tw/api/v4/search/search_items?by=sales&keyword={keyword}&limit=100&newest={s}&order=desc&page_type=search&scenario=PAGE_GLOBAL_SEARCH&version=2"
        res = requests.get(url)

        for item in res.json()['items']:
            if pd.isnull(item['adsid']):
                item_list.append(item['item_basic'])
        time.sleep(1)
    df = pd.DataFrame(item_list)

    # Data cleaning
    df_cleaned = df[['itemid', 'name', 'historical_sold', 'stock', 'price', 'liked_count', 'ctime']]
    df_cleaned['rating_star']  = df['item_rating'].apply(lambda x: round(x['rating_star'], 2))
    df_cleaned['rating_count']  = df['item_rating'].apply(lambda x: sum(x['rating_count']))
    df_cleaned['price'] = df_cleaned['price'].apply(lambda x: x/100000)
    df_cleaned['ctime'] = df_cleaned['ctime'].apply(lambda x: datetime.fromtimestamp(x))
    df_cleaned['name'] = df_cleaned.name.apply(lambda x: ''.join([i for i in x if i not in punctuation]))
    df_cleaned['category'] = '果苗'
    # drop duplicatd data 
    df_cleaned = df_cleaned[~df_cleaned.duplicated()]

    # Add updated datetime
    df_cleaned['updated_on'] = pd.Timestamp.now()


    # Update to bigquery 

    client = bigquery.Client()

    project_id = 'vaulted-quarter-352012'
    dataset_id = 'plants_web_crawler'
    table_id = 'table_v2'
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.ignore_unknown_values = True

    job = client.load_table_from_dataframe(
            df_cleaned,
            table_ref,
            job_config=job_config
        )

    table = client.get_table(f'{project_id}.{dataset_id}.{table_id}')
    print(f"已存入{table.num_rows}筆資料到{f'{project_id}.{dataset_id}.{table_id}'}")



if __name__=='__main__':
    print('running...')
    main()
    print('done!')