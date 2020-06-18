import sys
sys.path.append("..")
from pydruid.client import PyDruid
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from multiprocessing import Pool, cpu_count
from functools import partial
from dycounter.connection import Connector
from dycounter.pools import mongopool
from dycounter.config import ConfigManager as cm
import os, time, json, traceback
import requests
import pandas as pd
import pytz

flatten = lambda l: [item for sublist in l for item in sublist]

class BaseExecutor(object):
    """
    Base class for executors
    """

    def __init__(self):
        pass

    def run(self, start_time, end_time):
        time.sleep(6)
        return True

class OmsCounter(BaseExecutor):
    """
    A dynamic counter for FDS
    """

    DEFAULT_LOOK_BACK_SECONDS = 3600 #1 hour

    def __init__(self):
        pass

    def get_actived_rule(self):
        """
        Get all activated rules inside db stores 
        """
        mongo = mongopool.get()
        self.actived_rules = list(mongo.get_database("test").get_collection("count_rule").find({'is_active': True}))
        mongopool.put(mongo)

    def request_data(self, query):
        """
        Request data from druid with a query statement
        """
        return requests.post(
            "{}/{}".format(cm.FRAUD_DRUID_URL, cm.FRAUD_DRUID_PATH),
            auth = HTTPBasicAuth(cm.FRAUD_DRUID_USER, cm.FRAUD_DRUID_PASS),
            headers = {"content-type": "application/json; charset=UTF-8"},
            data = json.dumps(query)
        )

    def select_batch(self):
        """
        Request data inside a time range input
        """
        query = {
            "queryType": "scan",
            "dataSource": cm.FRAUD_DRUID_OMS_SOURCE,
            "resultFormat": "compactedList",
            "columns":[],
            "intervals": [
                "{}/{}".format(self.stime.strftime("%Y-%m-%dT%H:%M:%S%z"), self.etime.strftime("%Y-%m-%dT%H:%M:%S%z"))
            ],
            "batchSize": 20480
        }
        resp = self.request_data(query)
       
        try:
            columns = resp.json()[0]["columns"]
            data = flatten(map(lambda x: x["events"], resp.json()))
            df = pd.DataFrame(data, columns=columns)
            for col in ['products', 'seller_ids', 'cate2_ids', 'skus']:
                df[col] = df[col].apply(lambda x: x if type(x) is list else [x])
            # df["customer"] = df["customer"].apply(lambda x: json.loads(x))
            # df["products"] = df["products"].apply(lambda x: [json.loads(y) for y in x])
            # df['shipping'] = df['shipping'].apply(lambda x: json.loads(x))
            return df
        except:
            traceback.print_exc()
            return None

    def get_time_range_for_lookback(self, rule):
        """
        Generate time range for aggregation based on each rule
        """
        end_batch = self.etime.strftime("%Y-%m-%dT%H:%M:%S%z")
        if rule["type"] == "eod":
            start_batch = self.etime.strftime("%Y-%m-%dT00:00:00%z")
        else: 
            start_time = self.etime + timedelta(seconds=-rule.get("look_back_seconds", self.DEFAULT_LOOK_BACK_SECONDS))
            start_batch = start_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        return (start_batch, end_batch)

    def gen_filters(self, high_level, base_level, base_df):
        return [
            {
                "type": "in",
                "dimension": i,
                "values": list(set(flatten(base_df[i].to_list())))
            } for i in high_level
        ] + [
            {
                "type": "in",
                "dimension": i,
                "values": base_df[i].to_list()
            } for i in base_level
        ]

    def gen_dimensions(self, high_level, base_level, base_df):
        return [
            {
                "type": "listFiltered",
                "delegate": {
                    "type": "default",
                    "dimension": i,
                    "outputName": i[:-1]
                },
                "values": list(set(flatten(base_df[i].to_list())))
            } for i in high_level
        ] + [
            {
                "type": "in",
                "dimension": i,
                "values": list(set(base_df[i].to_list()))
            } for i in base_level
        ]

    def gen_agg_filters(self, high_level, base_level, base_df):
        """
        Generate filter for aggregation 
        """
        agg_filter = {
            "type" : "and",
            "fields": []
        }
        agg_filter["fields"].extend(
            [
                {"type" : "in", "dimension" : s, "values" : list(set(flatten(base_df[s].to_list())))} for s in high_level
                # for v in set(flatten(base_df[s].to_list()))
            ] + [
                { "type": "not", "field": {"type": "selector", "dimension": s, "value": None}} for s in high_level
            ]
        )
        agg_filter["fields"].extend([
            {
                "type": "in",
                "dimension": s,
                "values": list(set(base_df[s].to_list()))
            } for s in base_level
        ])
        return agg_filter

    def gen_agg_query(self, base_df, rule, start_batch, end_batch):
        """
        Generate a aggregation query for each rule
        """
        agg_filter = self.gen_agg_filters(rule["high_level"], rule["base_level"], base_df)
        query = {
            "queryType": "groupBy",
            "context": {
                "groupByStrategy": "v2"
            },
            "dataSource": cm.FRAUD_DRUID_OMS_SOURCE,
            "granularity": {"type": "all"},
            "intervals": [
                "{}/{}".format(start_batch, end_batch)
            ],
            "dimensions": [],
            "filter": {
                "type": "or",
                "fields": [
                    rule.get("filter", {"type": "true"})
                ]
            },
            "aggregations": [
                {
                    "type" : "filtered",
                    "filter" : agg_filter,
                    "aggregator": {"type": "count", "name": "count"}
                },
                # {
                #     "type": "javascript",
                #     "name": "related_orders",
                #     "fieldNames": ["order_code"],
                #     "fnAggregate": """function(current, order_code) {
                #         var code = order_code.toString();
                #         if (isNaN(current) || current == 0) { return code; }
                #         return current.toString().concat(code);
                #     }""",
                #     "fnCombine": """function(partialA, partialB) {
                #         if (isNaN(partialA) || partialA == 0) {
                #             if (isNaN(partialB) || partialB == 0) return "";
                #             return partialB.toString();
                #         }else {
                #             if (isNaN(partialB) || partialB == 0) return partialA.toString();
                #             return partialA.toString().concat(partialB.toString()); 
                #         }
                #     }""",
                #     "fnReset": "function() { return ""; }"
                # }
            ],
            "having": {
                "type": "greaterThan",
                "aggregation": "count",
                "value": 0
            }
        }
        # query["dimensions"].extend(self.gen_dimensions(inner, outer, base_df))
        query["dimensions"].extend([{"type": "default", "dimension": s, "outputName": s[:-1]} for s in rule['high_level']] + list(rule['base_level']))
        return query

    def get_related_orders(self, rule, start_batch, end_batch, agg_row):
        """
        Query related orders inside lookback time range
        """
        if (agg_row['count'] < rule["threshold_cancel"]): return None
        query = query = {
            "queryType": "scan",
            "dataSource": cm.FRAUD_DRUID_OMS_SOURCE,
            "resultFormat": "compactedList",
            "columns": ["order_code"],
            "filter": {
                "type": "and",
                "fields": [
                    rule.get("filter", {"type": "true"})
                ]
            },
            "intervals": [
                "{}/{}".format(start_batch, end_batch)
            ],
            "batchSize": 20480
        }
        query["filter"]["fields"].extend(
            [{"type": "selector", "dimension": s, "value": agg_row[s[:-1]]} for s in rule["high_level"]] +\
                [{"type": "selector", "dimension": s, "value": agg_row[s]} for s in rule["base_level"]])
        resp = self.request_data(query)
        try:
            data = flatten(map(lambda x: x['events'], resp.json()))
        except:
            traceback.print_exc()
            return None
        
        if type(data[0]) is list: data = flatten(data)
        if data: return data
        return None

    def agg_with_rule(self, base_df, end_time, rule):
        # print("Agg with rule: {}\n".format(rule))
        rule["high_level"] = set(rule["group_fields"]).intersection({"seller_ids", "skus", "cate2_ids"})
        rule["base_level"] = set(rule["group_fields"]) - rule["high_level"]
        start_batch, end_batch = self.get_time_range_for_lookback(rule)
        query = self.gen_agg_query(base_df, rule, start_batch, end_batch)
        resp = self.request_data(query)
        try:
            data = list(map(lambda x: x['event'], resp.json()))
            if data:
                agg_df = pd.DataFrame(data)
                related_getter_fn = partial(self.get_related_orders, rule, start_batch, end_batch)
                agg_df['related'] = agg_df.apply(related_getter_fn, axis=1)
            else:
                agg_df = None
        except:
            traceback.print_exc()
            agg_df = None
        return (rule, agg_df)

    def run(self, start_time, end_time):
        """
        Aggreate all rules at current time
        """
        self.stime = start_time
        self.etime = end_time
        self.get_actived_rule()
        base_df = self.select_batch()
        if base_df is None:
            return False
        
        def merge(aggs, base_row):
            count = {}
            for rule, agg_df in aggs:
                if agg_df is not None:
                    def checker():
                        ck = pd.Series([True]*agg_df.shape[0])
                        for s in rule['high_level']:
                            ck = ck & (agg_df[s[:-1]].isin(base_row[s]))
                        for s in rule['base_level']:
                            ck = ck & (agg_df[s] == base_row[s])
                        return ck

                    agg_map_js = agg_df[checker()].to_dict(orient="records")
                    if agg_map_js:
                        if rule['high_level']:
                            count[rule['count_field']] = agg_map_js
                        else:
                            count[rule['count_field']] = agg_map_js[0]
                    else:
                        count[rule['count_field']] = None
                else:
                    count[rule['count_field']] = None
            return count

        agg_func = partial(self.agg_with_rule, base_df, end_time)
        pool = Pool(cpu_count())
        aggs = pool.map(agg_func, self.actived_rules)
        base_merge_fn = partial(merge, aggs)
        st = time.time()
        base_df['counts'] = base_df.apply(base_merge_fn, axis=1)
        final = base_df.to_dict(orient='records')
        print("Merge time: {}\n".format(time.time() - st))
        print(final)
        return True

if __name__ == "__main__":
    start_time = datetime.strptime('2020-06-05 15:00:00+0700', '%Y-%m-%d %H:%M:%S%z')
    end_time = datetime.strptime('2020-06-05 15:00:10+0700', '%Y-%m-%d %H:%M:%S%z')
    # end_time = datetime.now().astimezone(pytz.timezone("Asia/Saigon")) + timedelta(days=-2)
    # start_time = end_time + timedelta(seconds=-10)
    counter = OmsCounter()
    st = time.time()
    counter.run(start_time, end_time)
    print(time.time() - st)