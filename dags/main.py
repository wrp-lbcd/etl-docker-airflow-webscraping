import pandas as pd
import requests
import os
from bs4 import BeautifulSoup
from airflow.decorators import task, dag
from datetime import datetime, date, timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook

#Arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,1,1)
}

#Config
BUCKET_NAME = 'bucket-th'
GCP_CONN_ID = 'google_cloud_default'
LOCAL_FILE = '/tmp/homepro_electric.csv'
DESTINATION_BLOB_NAME = 'data/scraping.csv'

@task()
def scraping_data():
    data = []

    for page in range(1, 5):
        URL = f'https://www.homepro.co.th/c/APP01?dcx=d&s=12&size=100&q=%E0%B9%80%E0%B8%84%E0%B8%A3%E0%B8%B7%E0%B9%88%E0%B8%AD%E0%B8%87%E0%B8%9B%E0%B8%A3%E0%B8%B1%E0%B8%9A%E0%B8%AD%E0%B8%B2%E0%B8%81%E0%B8%B2%E0%B8%A8&page={page}'
        response = requests.get(URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        products = soup.find_all('div', class_='plp-card-bottom')
        
        for i in products:
            try:
                item_info = i.find('div', class_='item-info')
                price_info = i.find('div', class_='item-price')

                sku_text = i.find('div', class_='sku').text.strip()
                sku = sku_text.replace('SKU:', '').strip()

                brand = item_info.find('div', class_='brand').text.strip()
                title = item_info.find('div', class_='item-title').text.strip()

                discounted_price = 0
                sell_span = price_info.find('span')
                if sell_span:
                    price_text = sell_span.text.replace('฿', '').replace(',', '').strip()
                    discounted_price = int(price_text) if price_text.isdigit() else 0

                full_price = 0
                dis_price = price_info.find('span', class_='dis-price')
                if dis_price:
                    full_text = dis_price.text.replace('฿', '').replace(',', '').strip()
                    full_price = int(full_text) if full_text.isdigit() else 0

                data.append({
                    'SKU': sku,
                    'Brand': brand,
                    'Title': title,
                    'Discounted_Price': discounted_price,
                    'Full_Price': full_price
                })
            except Exception as e:
                print(f"Error parsing item: {e}")

    df = pd.DataFrame(data)
    df.index = range(1, len(df) + 1)
    df.index.name = "No"
    df.to_csv(LOCAL_FILE, encoding="utf-8-sig", index=True)
    print("Scraping completed! File")

@task()
def load_to_gcs():
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=DESTINATION_BLOB_NAME,
        filename=LOCAL_FILE,
    )

#Dag
@dag(default_args=default_args, catchup=False)
def webscraping_pipeline(): 

    t1 = scraping_data()
    t2 = load_to_gcs()

    t1 >> t2

dag = webscraping_pipeline()