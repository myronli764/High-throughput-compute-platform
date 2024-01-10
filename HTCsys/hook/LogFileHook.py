import os
import sys
from utils.logger import logger
import time

class Log():
    def __init__(self,log_file,event_flag):
        self.log = log_file
        self.event_flag = event_flag
        self.on_hook = False

    def on_log_event(self,line):
        r"""you could define your own event action here
        use if statement in python and the specific log
        information format.
        logger_format = '%(asctime)s -- [%(levelname)s] -- (%(filename)s:%(lineno)d): %(message)s'
        """
        info = line.split('--')
        event_name = info[1].split()[0]
        if event_name == '[ERROR]':
            print(line)
            self.event_flag.set()
            self.stop_hook()
        if event_name in ['[INFO]','[DEBUG]']:
            pass
        if event_name == '[WARNING]':
            print(line)

    def Listener(self,log_file):
        logger.info('***  Listener in LogFileHook is on  ***' )
        while self.on_hook:
            with open(log_file,'r') as file:
                lines = file.readlines()
                file.seek(0,2)
                line = file.readline().rstrip('\n')
                if lines[-1].rstrip('\n'):
                    self.on_log_event(lines[-1])
                else:
                    time.sleep(0.1)

    def stop_hook(self):
        self.on_hook = False
        #exit(0)