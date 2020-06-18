import pandas as pd
import pytz
import gc
import json
from pymemcache.client import base
from pymemcache import fallback
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta
import time

PROJECT_ID = 'tiki-dwh'
credentials = service_account.Credentials.from_service_account_file(
    'credentials.json', scopes=['https://www.googleapis.com/auth/bigquery'])
sql = """SELECT
    *
FROM `tiki-dwh.sherlock.location_scoring_offline_*`
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL 1 DAY))
"""

class DataPool(object):
    data = None
    insert_day = None
    expire_time = None

    @classmethod
    def acquire(cls):
        current_time = datetime.now().astimezone(pytz.timezone("Asia/Saigon"))
        current_day = current_time.day
        if cls.insert_day is None:
            cls.insert_day = current_day
            cls.data = cls.query_data()
        elif current_day > cls.insert_day:
            cls.clear()
            cls.insert_day = current_day
            cls.data = cls.query_data()
        return cls.data

    @classmethod
    def acquire1(cls):
        current_time = datetime.now().astimezone(pytz.timezone("Asia/Saigon"))
        expire_time = datetime.strptime(current_time.strftime("%Y-%m-%dT%H:%M-%S%z"), "%Y-%m-%dT%H:%M:%S%z")
        if cls.expire_time is None:
            cls.expire_time = expire_time
            cls.data = cls.query_data()
        elif current_time > cls.expire_time:
            cls.clear()
            cls.expire_time = expire_time
            cls.data = cls.query_data()
        return cls.data

    @classmethod
    def test(cls):
        current_time = datetime.now().astimezone(pytz.timezone("Asia/Saigon"))
        expire_time = current_time + timedelta(seconds=5)
        if cls.expire_time is None:
            cls.expire_time = expire_time
            cls.data = current_time
        elif current_time > cls.expire_time:
            cls.clear()
            cls.expire_time = expire_time
            cls.data = current_time
        return cls.data


    @classmethod
    def clear(cls):
        temp = cls.data
        cls.data = None
        del temp
        gc.collect()

    @classmethod
    def query_data(cls):
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
        return client.query(sql, project=PROJECT_ID).to_dataframe()

if __name__ == '__main__':
    data = DataPool.acquire1()
    print(data)
    time.sleep(6)
    data = DataPool.acquire1()
    print(data)
