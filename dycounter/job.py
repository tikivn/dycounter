import sys
sys.path.append("..")
from datetime import datetime, timedelta
from croniter import croniter
from threading import Thread
from dycounter import counter, baser
from dycounter import configmanager as cm
from dycounter.pools import mongopool
from dycounter.utils import fullname
import pytz, time

class BaseJob(object):
    next_run_time = None
    is_jobtory_empty = True

    def __new__(cls, *args, **kwargs):
        return super(BaseJob, cls).__new__(cls)

    def __init__(self, identifier, interval, unit, scheduler, executor=counter, backfill=False, delay=0):
        self.load_config()
        self.now = datetime.now().astimezone(pytz.timezone(cm.DEFAULT_TIMEZONE))
        self.identifier = identifier
        self.interval = interval
        self.unit = unit
        self.cron = croniter(scheduler, self.now)
        self.executor = executor
        self.is_backfill = backfill
        self.delay = delay
        self.prepare()

    def prepare(self):
        if self.next_run_time is None:
            job = self.jobtory.find_one({'_id': self.identifier})
            self.prev_run_time = self.delayed(self.cron.get_prev(datetime))
            self.next_run_time = self.delayed(self.cron.get_next(datetime))
            if job:
                self.is_jobtory_empty = False
                last_run_time = pytz.utc.localize(job['last_run_time'], is_dst=None) \
                    .astimezone(pytz.timezone(cm.DEFAULT_TIMEZONE))
                self.backfill(last_run_time, self.prev_run_time)
        print("INIT {} - {} - {}\n".format(
            self.now.strftime("%Y-%m-%d %H:%M:%S%z"),
            self.prev_run_time.strftime("%Y-%m-%d %H:%M:%S%z"),
            self.next_run_time.strftime('%Y-%m-%d %H:%M:%S%z')
        ))

    def load_config(self):
        mongo = mongopool.get()
        self.jobtory = mongo.get_database('scheduler').get_collection('jobs')
        self.logtory = mongo.get_database('scheduler').get_collection('logs')

    def process(self, fn, args=()):
        t = Thread(target=fn, args=args)
        t.daemon = True
        t.start()

    def backfill(self, start_time, end_time):
        self.process(self.run_backfill, (start_time, end_time))

    def delayed(self, t):
        if self.delay == 0: return t
        if self.unit == 'second':
            return t + timedelta(seconds=-self.delay)
        if self.unit == 'minute':
            return t + timedelta(minutes=-self.delay)
        if self.unit == 'hour':
            return t + timedelta(hours=-self.delay)
        if self.unit == 'day':
            return t + timedelta(days=-self.delay)
        raise ValueError('Unsupported unit: {}'.format(self.unit))

    def run_backfill(self, start_time, curr_time):
        msg = 'BACKFILL from {} to {}'.format(
            start_time.strftime('%Y-%m-%d %H:%M:%S%z'),
            curr_time.strftime('%Y-%m-%d %H:%M:%S%z')
        )
        print(msg)

        status = None
        atype = 'backfill'
        if self.is_backfill:
            if curr_time > start_time:
                resp = self.executor.run(start_time, curr_time)
                if resp: status = 'succeeded' 
                else: status = 'failed'
        else:
            status = 'ignored'
        if status:
            self.update_job(curr_time)
            self.insert_log(atype, status, start_time, curr_time)

    def __call__(self):
        self.process(self.run_batch)
        
    def run_batch(self):
        self.now = datetime.now().astimezone(pytz.timezone(cm.DEFAULT_TIMEZONE))
        atype = 'batch'
        print('RUNBATCH {} - {} - {}'.format(
            self.now.strftime('%Y-%m-%d %H:%M:%S%z'), 
            self.prev_run_time.strftime('%Y-%m-%d %H:%M:%S%z'), 
            self.next_run_time.strftime('%Y-%m-%d %H:%M:%S%z'))
        )
        resp = self.executor.run(self.prev_run_time, self.next_run_time)
        if resp: status = 'succeeded'
        else: status = 'failed'
        self.update_job(self.next_run_time)
        self.insert_log(atype, status, self.prev_run_time, self.next_run_time)
        self.prev_run_time = self.next_run_time
        self.next_run_time = self.delayed(self.cron.get_next(datetime))
        msg = 'Update: {} - {}\n'.format(
            self.prev_run_time.strftime('%Y-%m-%d %H:%M:%S%z'), 
            self.next_run_time.strftime('%Y-%m-%d %H:%M:%S%z')
            )
        print(msg)
    
    def update_job(self, curr_time):
        if self.is_jobtory_empty:
            self.jobtory.insert_one({
                '_id': self.identifier, 
                'last_run_time': curr_time,
                'last_checking': self.now,
                'class': fullname(self.executor)
            })
            self.is_jobtory_empty = False
        else:
            self.jobtory.update_one({'_id': self.identifier}, { "$set": {
                    'last_run_time': curr_time,
                    'last_checking': self.now
                }
            })

    def insert_log(self, atype, status, from_time, to_time):
        self.logtory.insert_one({
            'identifier': self.identifier,
            'time': self.now,
            'type': atype,
            'from': from_time,
            'to': to_time,
            'status': status
        })