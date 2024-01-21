import json
import random
from typing import List, Dict
from utils.logger import logger
import uuid

class WorkInfo():
    def __init__(self,workinfo:Dict = {}):
        all_attr = ['name','idx','input','output','link','RunScript','state','pid_in_CNode','cwd']
        for k,v in workinfo.items():
            self.__setattr__(k,v)
        if workinfo.get('name') is None:
            self.name = uuid.uuid4()
        if workinfo.get('idx') is None:
            self.idx = ''
            logger.warning('Generate idx by graph algorithm.')
        if workinfo.get('input') is None:
            self.input = []
        if workinfo.get('output') is None:
            self.output = []
        if workinfo.get('link') is None:
            self.link = ['0->0']
            logger.warning('No link, set to 0->0, meaning single node.')
        if workinfo.get('RunScript') is None:
            self.RunScript = ''
        if workinfo.get('state') is None:
            self.state = 'ALIVE'
        if workinfo.get('pid_in_CNode') is None:
            self.pid_in_CNode = 0
        if workinfo.get('cwd') is None:
            self.cwd = '~'
        logger.info('workinfo get.')
    def __repr__(self) -> str:
        attrs = ['name', 'idx', 'input', 'output', 'link', 'RunScript', 'state', 'pid_in_CNode', 'cwd']
        return "WorkInfo(%s,%s,%s,%s,%s,%s,%s,%s,%s)\n" % tuple([f'{k}={self.__getattribute__(k)}' for k in attrs])

class CompNode():
    def __init__(self,compnode:Dict = {}):
        all_attr = ['nodeidx','nodename','username','hostname','port','key','pkey']
        for k,v in compnode.items():
            self.__setattr__(k,v)
        if compnode.get('nodeidx') is None:
            self.nodeidx = random.randint(0,10000)
            logger.warning('No nodeidx, set to random idx %d' % self.nodeidx)
        if compnode.get('nodename') is None:
            self.nodename = f'node-{uuid.uuid4()}'
        if compnode.get('username') is None:
            self.username = ''
            logger.error('No username.')
            raise
        if compnode.get('hostname') is None:
            self.hostname = ''
            logger.error('No hostname.')
            raise
        if compnode.get('port') is None:
            self.port = ''
            logger.error('No port.')
            raise
        if compnode.get('key') is None and compnode.get('pkey') is None:
            self.key = ''
            self.pkey = ''
            self.loggin = ''
            logger.error('No key and pkey.')
            raise
        if compnode.get('key') is None :
            self.key = ''
            logger.info('use pkey to loggin')
            self.loggin = 'pkey'
        if compnode.get('pkey') is None:
            self.pkey = ''
            logger.info('use key to loggin.')
            self.loggin = 'key'

    def __repr__(self) -> str:
        attrs = ['nodeidx','nodename','username','hostname','port','key','pkey']
        return "CompNode(%s,%s,%s,%s,%s,%s,%s)\n" % tuple([f'{k}={self.__getattribute__(k)}' for k in attrs])

class Logger():
    def __init__(self,NodeInfo:Dict = {},period:str = '10',):
        self.WorkNodeInfo = WorkInfo(NodeInfo['WorkNode'])
        self.CompNodeInfo = CompNode(NodeInfo['CompNode'])
        self.period = period
    def __repr__(self):
        return "Logger(%s,%s)\n" % (f'WorkNodeInfo={self.WorkNodeInfo.__repr__()}',f'period={self.period}')


if __name__ == '__main__':
    workinfo = WorkInfo({"state":"ALIVE","input":["zxz",],"output":"some files","idx":1,"link":["1->2",],
                    "RunScript":'gmx mdrun -deffnm lmy/test/Run_Data/trial/test -v -c lmy/test/Run_Data/trial/test.gro -ntmpi 1 -ntomp 12 -gpu_id 3',
                    "pid_in_CNode":0,"cwd":'/home','name':1})
    print(workinfo.__repr__())
    compnode = CompNode({'nodename':'node1','username':'shirui','hostname':'10.10.2.126','port':22,'key':'tony9527'})
    print(compnode.__repr__())
    Log = Logger(NodeInfo={'WorkNode':{"state":"ALIVE","input":["zxz",],"output":"some files","idx":1,"link":["1->2",],
                    "RunScript":'gmx mdrun -deffnm lmy/test/Run_Data/trial/test -v -c lmy/test/Run_Data/trial/test.gro -ntmpi 1 -ntomp 12 -gpu_id 3',
                    "pid_in_CNode":0,"cwd":'/home','name':1},
                           'CompNode':{'nodename':'node1','username':'shirui','hostname':'10.10.2.126','port':22,'key':'tony9527'}})
    print(Log.__repr__())