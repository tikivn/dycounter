import sys
sys.path.append("..")
from croniter import croniter
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.triggers.combining import AndTrigger,OrTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_ALL, EVENT_JOB_SUBMITTED, EVENT_JOB_ERROR
from multiprocessing import cpu_count
from dycounter.pools import mongopool
import dycounter.config as cf
import logging
import pytz

jobstores = {
    'default': MongoDBJobStore(host='localhost', port=27017)
}

executors = {
    'default': ThreadPoolExecutor(cpu_count()),
    'processpool': ProcessPoolExecutor(cpu_count())
}

job_defaults = {
    'coalesce': False,
    'max_instances': 3
}

logger = logging.getLogger('apscheduler.executors.default')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-10s %(module)-15s %(message)s'
)

class JobScheduler(object):

    def __init__(self, every=30, unit='second'):
        self.mongo = mongopool.get()
        self.cursor = self.mongo.get_database('apscheduler').get_collection('jobs')
        self.every = every
        self.unit = unit
        self.scheduler = BlockingScheduler(logger=logger)
        self.scheduler.configure(
            jobstores=jobstores, 
            executors=executors, 
            job_defaults=job_defaults, 
            timezone=pytz.timezone('Asia/Saigon'))
        self._set_trigger(every, unit)

    def _set_trigger(self, every, unit):
        now = datetime.now().astimezone(pytz.timezone('Asia/Saigon'))
        if unit == 'second':
            self.trigger=CronTrigger(second='*/{}'.format(every), start_date=now)
        elif unit == 'minute':
            self.trigger=CronTrigger(minute='*/{}'.format(every), start_date=now)
        elif unit == 'hour':
            self.trigger=CronTrigger(hour='*/{}'.format(every), start_date=now)
        elif unit == 'day':
            self.trigger=CronTrigger(day='*/{}'.format(every), start_date=now)
        else:
            raise Exception(message='Unknown time unit')

    def add_jobstore(self, jobstore, alias):
        self.scheduler.add_jobstore(jobstore, alias)

    def add_executor(self, executor, alias):
        self.scheduler.add_executor(executor, alias)

    def add_job(self, job_fn, id='id1', name='job1', jobstore='default', executor='default', args=None, kwargs=None):
        now = datetime.now().astimezone(pytz.timezone('Asia/Saigon'))
        history = list(self.cursor.find({'_id': id}))
        if history:
            #TODO: process missing jobs
            self.cursor.delete_one({'_id': id})
        next_run_time = self.trigger.get_next_fire_time(None, now)
        if kwargs:
            kwargs['run_time'] = next_run_time
        else:
            kwargs={
                'run_time': next_run_time
            }
            
        self.scheduler.add_job(job_fn,
            trigger=self.trigger,
            next_run_time=next_run_time,
            id=id, name=name,
            jobstore=jobstore,
            executor=executor,
            args=args,
            kwargs=kwargs)
     
        

    def remove_job(self, id, jobstore='default'):
        self.scheduler.remove_job(job_id=id, jobstore=jobstore)

    def callback(self, callback_fn, mark=EVENT_ALL):
        self.scheduler.add_listener(callback_fn)

    def start(self):
        mongopool.put(self.mongo)
        self.scheduler.start()

    def shutdown(self):
        self.scheduler.shutdown()
        self.scheduler.scheduled_job

import time
def test_fn(run_time):
    now = datetime.now().astimezone(pytz.timezone('Asia/Saigon'))
    time.sleep(6)
    print('{} - {}'.format(
        now.strftime('%Y-%m-%d %H:%M:%S%z'),
        run_time.strftime('%Y-%m-%d %H:%M:%S%z')
    ))

def test_callback(event):
    if event == EVENT_JOB_SUBMITTED:
        print('Job succeeded')
    else:
        print('Job crashed')

if __name__ == "__main__":
    scheduler = JobScheduler(10, 'second')
    scheduler.add_job(test_fn, id='id1', name='name1')
    scheduler.callback(test_callback, EVENT_JOB_SUBMITTED|EVENT_JOB_ERROR)
    scheduler.start()
