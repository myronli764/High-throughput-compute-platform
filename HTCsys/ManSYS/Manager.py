import os.path
import threading
import time
import shutil
import paramiko
from utils.logger import logger
from utils.Staff import WorkInfo,CompNode,Logger
from typing import List, Tuple, Union, Dict
import json
import networkx as nx
from DataBase.database import  WorkFlowDataBase, WorkNode, WorkFlow
from LaunchSYS.Launcher import Launcher
from LogSYS.WorkLog import *
import numpy as np

def dfs(G:nx.DiGraph,start,visited=None):
    if visited is None:
        visited = set()
        visited.add(start)
    for nei in G.neighbors(start):
        if nei not in visited:
            visited.add(nei)
        dfs(G,nei,visited)
    return visited
def bfs(G:nx.DiGraph,start,visited=None,visited_path=[]):
    if start == set():
        return visited_path
    if visited is None:
        visited = []
        visited.append(start)
    if visited_path == []:
        visited_path.append(start)
    if type(start) is int:
        start = {start}
    neis = set()
    neis_ = []
    for s in start:
        for n in G.neighbors(s):
            if n not in neis and n not in visited:
                neis.add(n)
                neis_.append(n)
                visited.append(n)
    visited_path.append(neis_)
    #print(visited,'*',neis)
    return bfs(G,neis,visited)
###  test for bfs
#G = nx.DiGraph()
#G.add_edges_from([(i,i+1) for i in range(5)])
#G.add_edges_from([(i,i+1) for i in range(6,8)])
#G.add_edges_from([(i,i+1) for i in range(9,10)])
#G.add_edges_from([(i,i+1) for i in range(12,14)])
#G.add_edge(1,2)
#G.add_edge(1,6)
#G.add_edge(7,9)
#G.add_edge(13,7)
#ini_nodes = []
#for n in G.nodes:
#    predecessor = G.predecessors(n)
#    if list(predecessor) == []:
#        ini_nodes.append(n)
#print(bfs(G,ini_nodes))
#import matplotlib.pyplot as plt
#pos = {
#    0:np.array([6,7]),1:np.array([6,6]),2:np.array([5,5]),3:np.array([5,4]),4:np.array([4,3]),5:np.array([4,2]),
#    6:np.array([7,5]),7:np.array([7,4]),8:np.array([6,3]),9:np.array([8,3]),10:np.array([8,2]),
#    11:np.array([9,7]),12:np.array([9,6]),13:np.array([8,5]),14:np.array([9,4])
#       }
#nx.draw(G,pos=pos,arrows=True,with_labels=True)
#plt.show()
#raise

def process_task(launcher:Launcher,path,mode='cluster',block=1):
    pid,_ = launcher.RunWorkNode(mode=mode,block=block)
    #print('+'*100,pid,path)
    f = open(os.path.join(path,f'{launcher.WorkNodeInfo.name}_pid.txt'),'w')
    f.write(pid)
    f.close()
    launcher.RunningDetect()
    return

def process_analtask(Analyzer:customlog,input=[],output=[],RunScript='',func=None):
    Analyzer.set_para(input=input,output=output,RunScript=RunScript,func=func)
    Analyzer.SetupLog()
    return

def process_logtask(Analyzer:Analyze,pid):
    Analyzer.set_para(input=[pid])
    Analyzer.SetupLog()
    return

class Adapter():
    def __init__(self,scheduling):
        self.ScheduleSystem = scheduling
        logger.info(f'The scheduling system is {self.ScheduleSystem}')

    def Command(self,script):
        Schedule = self.ScheduleSystem
        if Schedule == 'slurm':
            if script == 'run':
                script = 'sbatch'
            return script

#def WorknodeToInfo():


class CompNodeManager():
    r'''
    ## loggin protocol:
    >> workdict = {1:{"state":"ALIVE","input":"some files","output":"some files","idx":1,"link":["1->2",],"RunScript": 'echo hello_world'}
    >>           ,2:{"state":"ALIVE","input":"some files","output":"some files","idx":2,"link":["1->2",],"RunScript": 'echo hello_world'}}
    >> nlist = [
    >>            {'nodename':'my_pc','username':'shirui','hostname':'1.1.1.1','port':22,'key':'shirui','pkey':None},
    >>            {'nodename':'SuperComputer_center','username':'shirui','hostname':'md.me','port':22,'key':None,'pkey':'rsa.txt'},
    >>            ]
    >> m = Manager(workdict=workdict,CompNodesList=nlist)
    >> m.LogginProp()
    >> m.ConnectNode('my_pc')
    >> m.CloseNode('my_pc')
    '''



    ## set a process to get log data from nodes

    def LogginProp(self, hpc=False):
        r'''
        get a dict that for search compnode: nodeidx/nodename -> node
        :param hpc: if True use Sugon hpc for computing, default False
        :return:
        '''
        self.CompNodesList: List[CompNode,]
        if hpc is True:
            logger.info('Use resources from High Performance Computer Supercomputingcenter')
            self.CompNodes = {}
            for master in self.CompNodesList:
                self.CompNodes[master.nodeidx] = master
                self.CompNodes[master.nodename] = master
        else:
            logger.info('Use resources from Personal cluster')
            self.CompNodes = {}
            for node in self.CompNodesList:
                self.CompNodes[node.nodeidx] = node
                self.CompNodes[node.nodename] = node
        return



    def ConnectNodeTest(self,close=True):
        for info in self.CompNodesList:
            nodename = info.nodename
            info: CompNode
            hostname, username, port, key, pkey = (
                info.hostname, info.username, info.port, info.key, info.pkey)
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if info.loggin == 'key':
                try:
                    client.connect(hostname=hostname, username=username, port=port, password=key)
                    stdin, stdout, stderr = client.exec_command('echo "hello word!"')
                    logger.info(f'Successfully test connect to node {nodename}, say to it : {stdout.read().decode()} ')
                    self.ConnectedNodesList.append(nodename)
                    self.ConnectedClient[nodename] = client
                    self.ConnectedClient[info.nodeidx] = client
                    if close:
                        client.close()
                except paramiko.ssh_exception.AuthenticationException as e:
                    logger.error(f'FATAL ERROR for {nodename}: {e}')
            elif info.loggin == 'pkey':
                try:
                    private_key = paramiko.RSAKey.from_private_key_file(pkey)
                    client.connect(hostname=hostname, username=username, port=port, pkey=private_key)
                    stdin, stdout, stderr = client.exec_command('echo "hello word!"')
                    logger.info(f'Successfully connect to node {nodename}, say to it : {stdout.read().decode()} ')
                    self.ConnectedNodesList.append(nodename)
                    self.ConnectedClient[nodename] = client
                    self.ConnectedClient[info.nodeidx] = client
                    client.close()
                except paramiko.ssh_exception.AuthenticationException as e:
                    logger.error(f'COMPNODE: {nodename}, FATAL ERROR: {e}')
            else:
                logger.error(f'Failed to connect to node {nodename}. Please provide your key or public key to {nodename}.')



    def ConnectNode(self, nodename):
        info = self.CompNodes[nodename]
        info: CompNode
        hostname, username, port, key, pkey = (
        info.hostname, info.username, info.port, info.key, info.pkey)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if info.loggin == 'key':
            try:
                client.connect(hostname=hostname, username=username, port=port, password=key)
                stdin, stdout, stderr = client.exec_command('echo "hello word!"')
                logger.info(f'Successfully connect to node {nodename}, say to it : {stdout.read().decode()} ')
                self.ConnectedNodesList.append(nodename)
                self.ConnectedClient[nodename] = client
                self.ConnectedClient[info.nodeidx] = client
            except paramiko.ssh_exception.AuthenticationException as e:
                logger.error(f'FATAL ERROR: {e}')
        elif info.loggin == 'pkey':
            try:
                private_key = paramiko.RSAKey.from_private_key_file(pkey)
                client.connect(hostname=hostname, username=username, port=port, pkey=private_key)
                stdin, stdout, stderr = client.exec_command('echo "hello word!"')
                logger.info(f'Successfully connect to node {nodename}, say to it : {stdout.read().decode()} ')
                self.ConnectedNodesList.append(nodename)
                self.ConnectedClient[nodename] = client
                self.ConnectedClient[info.nodeidx] = client
            except paramiko.ssh_exception.AuthenticationException as e:
                logger.error(f'FATAL ERROR: {e}')
        else:
            logger.error(f'Failed to connect to node {nodename}. Please provide your key or public key to {nodename}.')



    def CloseNode(self, nodename):
        self.ConnectedClient[nodename].close()
        logger.info(f'close the client: {nodename}.')

    ## use this carefully
    def SetConnectNodesList(self,nodename):
        logger.warning('Custom set the connected nodes list, the node may not be test.')
        self.ConnectedNodesList.append(nodename)
        return

    ## set a Launch system,
    def SetLaunch(self,worknodeidx,compnodeidx):
        r'''
        set Launcher to self.Launchers:Dict, k = worknodeidx -> v = Launcher(worknode,compnode)
        :param worknodeidx: use worknodeidx/worknodename to specific a worknode
        :param compnodeidx: use compnodeidx/compnodename to specific a compnode
        :return:
        '''
        if not hasattr(self,'WorkFlow'):
            logger.error('You need to add WorkFlow to the Manager before set a launch')
            raise
        if not hasattr(self,'Launchers'):
            self.Launchers = {}
        self.WorkFlow.nodes[worknodeidx]['CompNode'] = self.CompNodes[compnodeidx]
        self.Launchers[worknodeidx] = Launcher(worknodeinfo=self.WorkFlow.nodes[worknodeidx]['WorkNode'],compnode=self.CompNodes[compnodeidx])
        self.Launchers.get(worknodeidx).SetClient(self.ConnectedClient[compnodeidx])
        logger.info(f'Set worknode-{worknodeidx} to compnode-{compnodeidx}.')
        return

    def RunLauncher(self,worknodeidx:int = None,block=1):
        self.Launchers.get(worknodeidx).RunWorkNode()
        self.WorkFlow.nodes[worknodeidx]['WorkNode'].state = self.Launchers.get(worknodeidx).STATE2RUNNING()
        #print(self.WorkFlow.nodes[worknodeidx]['WorkNode'].state)
        #print(self.Launchers.get(worknodeidx).GetRunStat())
        if block:
            self.Launchers.get(worknodeidx).RunningDetect()
        return

    def GetRunSTATE(self,worknodeidx):
        if self.Launchers.get(worknodeidx).RunningState():
            return 'COMPLETE'
        else:
            return 'RUNNING'

class Manager(CompNodeManager):
    def __init__(self,workjson:str = None , workdict : Dict = {}, CompNodesList: List[Dict,]=[],DataPadPath='.',pos={},name='workflow'):
        r'''

        :param workjson: json represented the work flow
        :param workdict: dictory represented the work flow
        :param nodeslist: list of available nodes, you should specific the information of the nodes into a Dict
        ## EXAMPLE:
                [
                {'nodename':'node1_name','username':'tony','hostname':'10.10.2.100','key':'abc123','pkey':None},
                {'nodename':'node2_name','username':'Jack','hostname':'10.10.2.101','key':None,'pkey':'/home/Jack/pub_rsa_key.txt'},
                ]
        '''
        self.name = name
        self.workjson = workjson
        self.workdict = workdict
        self.pos = pos
        if self.workjson is None and self.workdict is None:
            logger.error('Please offer your work json or work dictory.')
        if self.workdict is None:
            self.workdict = json.loads(self.workjson)
        logger.info('Has load workflow data.')
        self.CompNodesList = [CompNode(_) for _ in  CompNodesList]
        for i,_ in enumerate(self.CompNodesList):
            _.nodeidx = i
        #print(self.CompNodesList)
        if self.CompNodesList is []:
            logger .error('There are no computer resources in nodes list.')
        self.ConnectedNodesList = []
        self.ConnectedClient = {}
        self.CompleteWorkSet = set()
        databasepath = os.path.join(DataPadPath,'DataPad')
        if os.path.exists(databasepath):
            shutil.rmtree(databasepath)
        os.mkdir(databasepath)
        self.DataPadPath = databasepath
        self.stat = {}## {'workflow':,'database':,'launcher':}
        self.Loggers = {}
        self.Analyzer = {}
        self.is_log = 0
        self.is_analyze = 0


    def addWorkNode(self,workdict):
        self.workdict.update(workdict)


    def JsonToWorkGraph(self,):
        G_work = nx.DiGraph()
        for n,info in self.workdict.items():
            worknode = WorkNode(info)
            if worknode.cwd == '~':
                worknode.cwd = f'~/htcwork_{self.name}/worknode_{n}'
            worknode.UnifyRunScript()
            #print('RunScript:',worknode.RunScript)
            G_work.add_node(worknode.idx,WorkNode=worknode)
            edges = info['link']
            for e in edges:
                axes = ['->','<-']
                for a in axes:
                    if a in e:
                        axe = a
                if axe == '->':
                    parent, kid = e.split(axe)
                else:
                    kid, parent = e.split(axe)
                G_work.add_edge(int(parent),int(kid))
        self.WorkFlow = G_work
        return G_work


    def WorkFlowToDataBase(self):
        self.DataBase = WorkFlowDataBase(self.WorkFlow)
        return self.DataBase

    def WorkFlowFromDataBase(self,DataBase: WorkFlowDataBase):
        self.DataBase = DataBase
        self.WorkFlow = DataBase.workflow.WorkGraph
        return

    def JSONToWorkFlow(self):
        self.JsonToWorkGraph()
        self.WorkFlowToDataBase()

    def Update(self,worknodeids:set,state:str):
        for idx in worknodeids:
            self.WorkFlow.nodes[idx]['WorkNode'].state = state
            self.DataBase.workflow.WorkGraph.nodes[idx]['WorkNode'].state = state

    def TFileInCNodes(self,origin:int,opath:str,ofile,destination:int,dpath:str,dfile):
        client_orig = self.ConnectedClient[origin]
        client_dest = self.ConnectedClient[destination]
        client_orig: paramiko.SSHClient
        client_dest: paramiko.SSHClient
        if isinstance(ofile,list):
            for o,d in zip(ofile,dfile):
                stdin, stdout, stderr = client_orig.exec_command(f'cat {opath}/{o}')
                while not stdout.channel.exec_command():
                    time.sleep(1)
                content = stdout.read(1024).decode()
                stdin, stdout, stderr = client_dest.exec_command(f'echo {content} > {dpath}/{d}')
                while not stdout.channel.exec_command():
                    time.sleep(1)
                content = stdout.read(1024).decode()
        else:
            stdin, stdout, stderr = client_orig.exec_command(f'cat {opath}/{ofile}')
            while not stdout.channel.exec_command():
                time.sleep(1)
            content = stdout.read(1024).decode()
            stdin, stdout, stderr = client_dest.exec_command(f'echo {content} > {dpath}/{dfile}')
        return

    def ScanWorkFlow(self) :
        r'''
        ## this method is used to Scan the WorkFlow Graph, and get a list of 'READY' work, update the database and workflow
        ## improve : this algorithm is suitable for small graph, O(en)
        :return:
        '''
        ini_nodes = []
        digraph = {}
        for idx in self.WorkFlow.nodes:
            digraph[idx] = {}
            precessors = self.WorkFlow.predecessors(idx)
            successors = self.WorkFlow.successors(idx)
            digraph[idx]['parents'] = list(precessors)
            digraph[idx]['children'] = list(successors)
            if digraph[idx]['parents'] == []:
                ini_nodes.append(idx)
        launch_paths = bfs(self.WorkFlow,ini_nodes)
        for idxs in launch_paths:
            for idx in idxs:
                #print((self.WorkFlow.nodes[idx]['WorkNode']).state)
                if (self.WorkFlow.nodes[idx]['WorkNode']).state == 'COMPLETE':
                    self.CompleteWorkSet.add(idx)
        logger.info(f'The follow works have complete: {self.CompleteWorkSet}.')
        readypad_ = set()
        for idxs in launch_paths:
            for idx in idxs:
                if idx in self.CompleteWorkSet:
                    continue
                if (self.WorkFlow.nodes[idx]['WorkNode']).state != 'ALIVE':
                    continue
                parents = digraph[idx]['parents']
                children = digraph[idx]['children']
                if parents == []:
                    readypad_.add(idx)
                    continue
                isready = 1
                for pre in parents:
                    if (self.WorkFlow.nodes[pre]['WorkNode']).state != 'COMPLETE':
                        isready = 0
                if isready:
                    readypad_.add(idx)
        WorkIsDone = 0
        if readypad_ == set():
            WorkIsDone = 1
            self.LaunchPad = readypad_
            self.DataBase.dump(to_dir=self.DataPadPath,pos=self.pos)
            return WorkIsDone
        readypad = readypad_
        self.ReadyPad = readypad
        self.Update(readypad,'READY') # update for state: 'ALIVE' -> 'READY'
        self.DataBase.dump(to_dir=self.DataPadPath,pos=self.pos)
        return WorkIsDone

    def AllocateResources(self):
        r'''
        ## this method is used to Allocate the available resources to the specific Node
        ## need to be improved: give an algorithm to determine the mapping between compnode and worknode
        :return:
        '''
        n_connected = len(self.ConnectedNodesList)
        CNodesList = self.ConnectedNodesList
        n_ready = len(self.ReadyPad)
        DressedWNodes = []
        count_comp = 0
        getDress = set()
        for workidx in self.ReadyPad:
        ## set launch and pulselog
            if count_comp == n_connected:
                DressedWNodes.append(getDress)
                count_comp = 0
                getDress = set()
            CNodeidx = CNodesList[count_comp]
            self.SetLaunch(workidx,CNodeidx)
            self.set_PulseLog(workidx)
        ## get cwd dir

        ## get file
            if self.WorkFlow.nodes[workidx]['WorkNode'].cross_node_input:
                cross_worknode_idx = self.WorkFlow.nodes[workidx]['WorkNode'].cross_node_idx
                if CNodeidx == self.WorkFlow.nodes[cross_worknode_idx]['CompNode'].nodeidx:
                    input_list = []
                    for out in self.WorkFlow.nodes[workidx]['WorkNode'].input:
                        input_list.append(f'{self.WorkFlow.nodes[cross_worknode_idx]["WorkNode"].cwd}/{out}')
                    self.WorkFlow.nodes[workidx]['WorkNode'].input = input_list
                else:
                    cross_CNodeidx = self.WorkFlow.nodes[cross_worknode_idx]['CompNode'].nodeidx
                    cwd = self.WorkFlow.nodes[workidx]['WorkNode'].cwd
                    cross_cwd = self.WorkFlow.nodes[cross_worknode_idx]["WorkNode"].cwd
                    output = self.WorkFlow.nodes[workidx]['WorkNode'].input
                    self.TFileInCNodes(origin=CNodeidx,destination=cross_CNodeidx,opath=cwd,dpath=cross_cwd,ofile=output,dfile=output)
                    input_list = []
                    for out in self.WorkFlow.nodes[workidx]['WorkNode'].input:
                        input_list.append(f'{self.WorkFlow.nodes[workidx]["WorkNode"].cwd}/{out}')
                    self.WorkFlow.nodes[workidx]['WorkNode'].input = input_list

            getDress.add(workidx)
            count_comp +=1
        DressedWNodes.append(getDress)
        self.DressedWNodes = DressedWNodes
        return DressedWNodes


    def LaunchCNodes(self,c=0):
        r'''
        ## all things is ready, just run worknode in the compute node.
        ## is connect?
        ### parallel running
        :return:
        '''
        for dressednodes in self.DressedWNodes:
            for dressednode in dressednodes:
                if self.Launchers.get(dressednode).Client.get_transport() is None:
                    self.ConnectNode(self.WorkFlow.nodes[dressednode]['CompNode'].nodename)
                t = threading.Thread(target=process_task,args=(self.Launchers.get(dressednode),self.DataPadPath))
                #time.sleep(3)
                if not hasattr(self,'RunningThreading'):
                    self.RunningThreading = {}
                self.RunningThreading[dressednode] = t
                t.start()
                if self.Loggers.get(dressednode) is not None:
                    while not os.path.exists(os.path.join(self.DataPadPath,f'{self.WorkFlow.nodes[dressednode]["WorkNode"].name}_pid.txt')):
                        pass
                    f = open(os.path.join(self.DataPadPath,f'{self.WorkFlow.nodes[dressednode]["WorkNode"].name}_pid.txt'),'r')
                    pid = f.read()
                    f.close()
                    #os.rmdir(f'{self.WorkFlow.nodes[dressednode]["WorkNode"].name}_pid.txt')
                    t0 = threading.Thread(target=process_logtask,args=(self.Loggers.get(dressednode),pid))
                    if not hasattr(self,'RunningLogThreading'):
                        self.RunningLogThreading = {}
                        self.is_log = 1
                    self.RunningLogThreading[dressednode] = t0
                    t0.start()
                ## please do this analyze work if there are more than one analyzer
                if self.Analyzer.get(dressednode) is not None:
                    t1 = threading.Thread(target=process_logtask, args=(self.Loggers.get(dressednode)))
                    if not hasattr(self,'RunningAnalyzeThreading'):
                        self.RunningAnalyzeThreading = {}
                        self.is_analyze = 1
                    self.RunningAnalyzeThreading[dressednode] = t1
                    t1.start()
            self.Update(dressednodes,'RUNNING') # update for state: 'READY' -> 'RUNNING'
            self.DataBase.dump(to_dir=self.DataPadPath,filename=f'iter_{c}',pos=self.pos)
            [self.RunningThreading[t].join() for t in self.RunningThreading]
            if self.is_analyze:
                [self.RunningAnalyzeThreading[t].join() for t in self.RunningAnalyzeThreading]
            if self.is_log:
                [self.RunningLogThreading[t].join() for t in self.RunningLogThreading]
        for dressednodes in self.DressedWNodes:
            for dressednode in dressednodes:
                if self.GetRunSTATE(dressednode):
                    logger.info(f'WorkNode {dressednode} has done.')
                    self.Update({dressednode,},'COMPLETE')
                else:
                    logger.warning(f'WorkNode {dressednode} has failed. Turn to ERROR detour.')
                    ## detected the ERROR
        return

    def RunWorkFlow(self):
        WorkIsDone = self.ScanWorkFlow()
        if WorkIsDone:
            return
        c = 0
        while not WorkIsDone:
            self.AllocateResources()
            self.LaunchCNodes(c)
            WorkIsDone = self.ScanWorkFlow()
            c+=1
        self.ScanWorkFlow()
        return

    def set_PulseLog(self,worknodeidx):
        Log = Logger(dict(WorkNode=self.WorkFlow.nodes[worknodeidx]['WorkNode'],CompNode=self.WorkFlow.nodes[worknodeidx]['CompNode']))
        self.Loggers[worknodeidx] = Pulse(Log=Log)
        self.WorkFlow.nodes[worknodeidx]['PulseLogger'] = self.Loggers[worknodeidx]
        logger.info(f'Set a pulse Logger to worknode {worknodeidx}')
        return

    def set_analyze(self,worknodeidx,name,input=[],output=[],RunScript='',func=None):
        #self.is_analyze = 1
        self.Analyzer[worknodeidx] = Logger(dict(WorkNode=self.WorkFlow.nodes[worknodeidx]['WorkNode'],CompNode=self.WorkFlow.nodes[worknodeidx]['CompNode']))
        self.WorkFlow.nodes[worknodeidx]['Analyzers'] = customlog(Log=self.Loggers[worknodeidx],name=name)
        logger.info(f'Set a analyzer {self.WorkFlow.nodes[worknodeidx]["Analyze"].name} to worknode {worknodeidx}')
        return




