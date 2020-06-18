from dycounter import counter, baser
from dycounter.scheder import Scheder
from dycounter.job import BaseJob
from dycounter.config import ConfigManager as cm
from datetime import datetime, timedelta
from argparse import ArgumentParser
import logging
import pytz


def init_args():
    parser = ArgumentParser()
    parser.add_argument("-c", "--config", type=str, help="Spec config file", default='config.yaml')
    return parser.parse_args()

def run():
    Scheder.add_job(BaseJob, 'oms_counter', cm.BATCH_INTERVAL, cm.BATCH_UNIT, {'executor': counter, 'backfill': cm.AGG_BACKFILL, 'delay': cm.AGG_DELAY})
    Scheder.run()

if __name__ == '__main__':
    args = init_args()
    run()