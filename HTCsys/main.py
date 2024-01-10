import time

from utils.logger import logger
import numpy as np
import hook
from hook import LogFileHook
import threading

if __name__ == '__main__':
    r = np.random.random(100)
    log_file = 'file.log'
    global event_flag
    event_flag = threading.Event()
    myHook = LogFileHook.Log(log_file,event_flag)
    myHook.on_hook = True
    p = threading.Thread(target=myHook.Listener,args=(log_file,))
    #myHook.Listener(log_file)
    p.start()
    for i,n in enumerate(r):
        #logger.warning('test for warning')
        print(i)
        time.sleep(0.2)
        if i > 20 :
            logger.error('test for hook.')
            #myHook.stop_hook()
        else :
            logger.warning('test for warning')
        if event_flag.is_set():
            print('c u later')
            break
    p.join()
    print('for is done'+'*'*50)