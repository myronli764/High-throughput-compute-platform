import logging

logger_format = '%(asctime)s  [%(levelname)s]  (%(filename)s:%(lineno)d): %(message)s'
logging.basicConfig(format=logger_format, level=logging.INFO)
filename='E:/downloads/work/HTCsys/file.log'
def get_logger(name):
    return logging.getLogger(name)

class DuplicateFilter:
    """
    Filters away duplicate log messages.
    Modified version of: https://stackoverflow.com/a/31953563/965332
    """

    def __init__(self, logger):
        self.msgs = set()
        self.logger = logger

    def filter(self, record):
        msg = str(record.msg)
        is_duplicate = msg in self.msgs
        if not is_duplicate:
            self.msgs.add(msg)
        return not is_duplicate

    def __enter__(self):
        if len(self.logger.filters) == 0:
            self.logger.addFilter(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

logger = get_logger(__name__)
logger.setLevel('INFO')

if __name__ == '__main__':
    logger.debug("This is a debug message!")
    logger.info("This is a info message!")
    logger.warning("This is a warning message!")
    logger.error("This is a error message!")
    logger.critical("This is a critical message!")
    #print("2023-07-19 17:49:14,258 -- [CRITICAL] -- (logger.py:17): This is a critical message!".split('--')[1].split()[0])