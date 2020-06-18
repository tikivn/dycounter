from datetime import datetime
from dycounter.pools import mongopool
import json
import os

# only for the first time inserted rule for dev
def load_rule_from_file(file_name: str, mongo) -> dict:
    with open(file_name, 'r') as f:
        rule = json.load(f)
    rule["time_field"] = "__time"
    rule["start_date"] = datetime.now() 
    rule["created_date"] = datetime.now()
    rule["created_user"] = "cuong.vo2@tiki.vn"
    rule["updated_date"] = datetime.now()
    rule["updated_user"] = "cuong.vo2@tiki.vn"
    rule["last_checking"] = datetime.now()
    mongo.get_database("test").get_collection("count_rule").insert_one(rule)

def insert_rules():
    rule_dir = 'rules'
    rules = os.listdir(rule_dir)
    mongo = mongopool.get()
    for r in rules:
        print(r)
        load_rule_from_file(rule_dir + r, mongo)
    mongopool.put(mongo)

if __name__ == "__main__":
    insert_rules()
