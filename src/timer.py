import logging
import time
from collections import defaultdict

#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TimerStat:
    def __init__(self, timer_on=False):
        self._dict = defaultdict(int)
        self._cnt = 0
        self._timer_on = timer_on

    def put(self, func_name, elapsed):
        self._dict[func_name] += elapsed
        self._cnt += 1

    def get(self, func_name):
        if (self._cnt):
            return self._dict.get(func_name, 0) / self._cnt

timer_stat = TimerStat()


def measure_time(timer_stat):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        def wrapper_timer(*args, **kwargs):
            start_time = time.time_ns()
            result = func(*args, **kwargs)
            end_time = time.time_ns()
            exec_time = (end_time - start_time) / 1000  # to microsecond
            timer_stat.put(func.__name__, exec_time)
            #logging.info(f"Function '{func.__name__}' executed in {exec_time} us")
            return result

        if timer_stat._timer_on:
            return wrapper_timer
        else:
            return wrapper

    return decorator

