import sys
sys.path.append("..")
from dycounter.executor import BaseExecutor
from datetime import datetime
import time
import schedule


class Scheder(object):
    """
    An interface for schedule and run multi jobs
    """

    @staticmethod
    def add_job(job_cls, identifier, interval, unit, kwargs):
        """
        Function for adding new job
        params:
            job_cls: a Class of the job this inherit the BaseExecutor
            interval: the interval between 2 consecutive times running job
            unit: the unit of the interval, include: ('second', 'minute', 'hour', 'day')
            kwargs: additional parameters of init function of job_cls
        """
        scheduler = Scheder.get_scheduler(interval, unit)
        job = job_cls(identifier, interval, unit, scheduler, **kwargs)
        looper = schedule.every(interval)
        if unit == 'second':
            looper.seconds.do(job)
        elif unit == 'minute':
            looper.minutes.do(job)
        elif unit == 'hour':
            looper.hours.do(job)
        elif unit == 'day':
            looper.days.do(job)
        else:
            raise ValueError('Unsupported unit: {}'.format(unit))
    
    @staticmethod
    def run():
        """
        Function for run pending all scheduled jobs
        """
        while True:
            schedule.run_pending()

    @staticmethod
    def get_scheduler(interval, unit):
        if unit == 'second':
            return '* * * * * */{}'.format(interval)
        if unit == 'minute':
            return '*/{} * * * *'.format(interval)
        if unit == 'hour':
            return '0 */{}  * * *'.format(interval)
        if unit == 'day':
            return '0 0 */{} * *'.format(interval)
        raise ValueError('Unsupported unit: {}'.format(unit))