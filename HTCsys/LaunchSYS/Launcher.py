import paramiko
from utils.Staff import WorkInfo,CompNode
from utils.logger import logger
from DataBase.database import WorkNode
from typing import Dict
import time
import os
'''
nlist = [
         ## node1 =      {'nodename':'node1','username':'shirui','hostname':'10.10.2.126','port':22,'key':'tony9527','pkey':None},
         ## node2 =      {'nodename':'node2','username':'shirui','hostname':'tycs.nsccty.com','port':65091,'key':None,'pkey':'E:/downloads/work/HTCsys/public_key/tycs.nsccty.com_0108162129_rsa.txt'},
                ]
'''
class Launcher():
    ### do not sent the WorkNode to Launcher, just information is enough
    def __init__(self,worknodeinfo: WorkInfo,compnode:CompNode):
        r'''
        ### must remove the backspace in the right of the RunScript!!

        :param worknodeinfo: {"state":"ALIVE","input":["some files",],"output":"some files","idx":1,"link":["1->2",],"RunScript":'xxx',"pid_in_CNode":0,"cwd":'/home'}
        :param compnode: {'nodename':'node1','username':'shirui','hostname':'10.10.2.126','port':22,'key':'tony9527','pkey':None}
        '''
        self.WorkNodeInfo = worknodeinfo
        attrs = dir(compnode)
        for attr in attrs:
            if attr.startswith('__'):
                continue
            self.__setattr__(attr,compnode.__getattribute__(attr))
        if self.WorkNodeInfo is None:
            logger.error(f'there is no work in this compnode')
        if hasattr(self,'nodename') is None:
            raise logger.error(f'WorkNode {self.WorkNodeInfo.name} failed to allocate CompNode.')

    def STATE2ALIVE(self):
        ## return the state, but set by Manager
        logger.info(f'The work in node {self.nodename} is ALIVE.')
        return 'ALIVE'

    def STATE2READY(self):
        ## return the state, but set by Manager
        logger.info(f'The work in node {self.nodename} is READY.')
        return 'READY'

    def STATE2RUNNING(self):
        ## return the state, but set by Manager
        logger.info(f'The work in node {self.nodename} is RUNNING.')
        return 'RUNNING'

    def STATE2COMPLETE(self):
        ## return the state, but set by Manager
        logger.info(f'The work in node {self.nodename} is COMPLETE.')
        return 'COMPLETE'

    def STATE2ERROR(self):
        ## return the state, but set by Manager
        logger.info(f'The work in node {self.nodename} is ERROR.')
        return 'ERROR'

    def STATE2DEAD(self):
        ## return the state, but set by Manager
        logger.info(f'The work in node {self.nodename} is DEAD.')
        return 'DEAD'

    def SetClient(self,client:paramiko.SSHClient):
        self.Client = client
        #logger.info(f'Has set client for {self.nodename}.')
    def GetAbsPath(self,mode='cluster'):
        r'''
        only linux system, specific the infrastructure.
        :param mode: ['cluster','slurm'], mode='cluster' for default
        :return:
        '''
        if mode == 'cluster' :
            RunScript = self.WorkNodeInfo.RunScript
            pwd = '/usr/sbin/lsof -p `ps -ef | grep "%s" | grep -v grep | awk \'{print $2}\'` | grep cwd | awk \'{print $9}\'' % (RunScript)
            stdin, stdout, stderr = self.Client.exec_command(pwd)
            cwd = stdout.read().decode().replace('\n','')
            return cwd
        if mode == 'slurm':
            pwd = f'scontrol show job {self.WorkNodeInfo.pid_in_CNode} | grep WorkDir'
            stdin, stdout, stderr = self.Client.exec_command(pwd)
            cwd = stdout.read().decode().replace('\n','')
            return cwd


    def GetCommandInfo(self,mode='cluster'):
        if self.WorkNodeInfo.pid_in_CNode == 0:
            logger.error(f'Have not run the command {self.WorkNodeInfo.pid_in_CNode}')
            return 0
        if mode=='cluster':
            exe_info = f'/usr/sbin/lsof -p {self.WorkNodeInfo.pid_in_CNode} | grep cwd'
            stdin, stdout, stderr = self.Client.exec_command(exe_info)
            info = stdout.read().decode()
            if info == '':
                info = 'Not on running'
            else:
                info = 'On running'
            return info
        if mode == 'slurm':
            exe_info = f'squeue | grep {self.WorkNodeInfo.pid_in_CNode}'
            stdin, stdout, stderr = self.Client.exec_command(exe_info)
            info = stdout.read().decode()
            if info == '':
                info = 'Not on queue'
            else:
                info = 'On queue'
            return info

    def RunWorkNode(self,mode='cluster',block=1):
        logger.info(f'mode = {mode}.')
        if mode == 'cluster':
            channel1 = self.Client.get_transport().open_session()
            channel1.setblocking(block)
            channel1.exec_command(self.WorkNodeInfo.RunScript)
            self.RunChannel = channel1
            logger.info('get RunChannel to Run the work.')
            pid = "ps -ef | grep '%s' | grep -v grep | awk '{print $2}'" % (self.WorkNodeInfo.RunScript)
            channel = self.Client.get_transport().open_session()
            channel.setblocking(block)
            channel.exec_command(pid)
            time.sleep(1)
            pid,__ = (channel.recv(1024).decode(), channel.recv_stderr(1024).decode())
            pid = pid.replace('\n', '')
            self.WorkNodeInfo.pid_in_CNode = pid
            logger.info(f'get pid {pid} of the work in the Compute Node {self.nodename}.')
            ret = [pid,__]
            self.WorkNodeInfo.state = self.STATE2RUNNING()
            return ret
        if mode == 'sbatch':
            channel1 = self.Client.get_transport().open_session()
            channel1.setblocking(block)
            withpidcmd = "%s | awk '{print $4}'" % self.WorkNodeInfo.RunScript
            channel1.exec_command(withpidcmd)
            self.RunChannel = channel1
            logger.info('get RunChannel to Run the work.')
            time.sleep(10)
            pid = channel1.recv(1024).decode()
            _ = channel1.recv_stderr(1024).decode()
            pid = pid.replace('\n', '')
            self.WorkNodeInfo.pid_in_CNode = pid
            logger.info(f'get pid {pid} of the work in the Compute Node {self.nodename}.')
            ret = [pid,_]
            self.WorkNodeInfo.state = self.STATE2RUNNING()
            return ret

    def GetRunStat(self,mode='cluster'):
        stat = {}
        stat['state'] = self.WorkNodeInfo.state
        stat['pwd'] = self.GetAbsPath()
        stat['RunScript'] = self.WorkNodeInfo.RunScript
        stat['pid_in_CNode'] = self.WorkNodeInfo.pid_in_CNode
        if mode=='slurm' :
            stat['squeue'] = self.GetCommandInfo()
        stat_s = ''
        for k in stat:
            stat_s += f'{k}  : {stat[k]}  \n'
        return stat_s
    def RunningState(self):
        return self.RunChannel.exit_status_ready()
    def RunningDetect(self):
        while not self.RunChannel.exit_status_ready():
            time.sleep(1)
        self.stdout = self.RunChannel.recv(1024).decode()
        self.stderr = self.RunChannel.recv_stderr(1024).decode()
        return

    def KillRun(self,mode='cluster'):
        if mode == 'cluster':
            self.Client.exec_command('kill -9 %s' % self.WorkNodeInfo.pid_in_CNode)
        if mode == 'slurm':
            self.Client.exec_command('scancel %s' % self.WorkNodeInfo.pid_in_CNode)
        logger.info(f'Has kill the work in the Compute Node {self.nodename} with pid {self.WorkNodeInfo.pid_in_CNode}.')

if __name__ == '__main__':
    from ManSYS.Manager import Manager
    Cnode = {'nodename': 'node1', 'username': 'shirui', 'hostname': '10.10.2.126', 'port': 22, 'key': 'tony9527', 'pkey': None}
    ## 'gmx grompp -c lmy/test/Run_Data/ini.gro -f lmy/test/Run_Data/trial/trial.mdp -p lmy/test/Run_Data/topol.top -o lmy/test/Run_Data/trial/test -maxwarn 100'
    ## 'gmx mdrun -deffnm lmy/test/Run_Data/trial/test -v -c lmy/test/Run_Data/trial/test.gro -ntmpi 1 -ntomp 12 -gpu_id 3'
    Worknodeinfo = {"state":"ALIVE","input":["zxz",],"output":"some files","idx":1,"link":["1->2",],
                    "RunScript":'gmx mdrun -deffnm lmy/test/Run_Data/trial/test -v -c lmy/test/Run_Data/trial/test.gro -ntmpi 1 -ntomp 12 -gpu_id 3',
                    "pid_in_CNode":0,"cwd":'/home','name':1}
    Cnode = CompNode(Cnode)
    Wnode = WorkInfo(Worknodeinfo)
    launcher = Launcher(worknodeinfo=Wnode,compnode=Cnode)
    ## test for cluster
    workdict = {1: {"state": "ALIVE", "input": "some files", "output": "some files", "idx": 1, "link": ["1->2", ],
                    "RunScript": 'echo hello_world'}
        , 2: {"state": "ALIVE", "input": "some files", "output": "some files", "idx": 2, "link": ["1->2", ],
              "RunScript": 'echo hello_world'}}
    # s = json.dumps(workdict)
    # print(s)
    nlist = [
        {'nodename': 'node1', 'username': 'shirui', 'hostname': '10.10.2.126', 'port': 22, 'key': 'tony9527',
         'pkey': None},
        {'nodename': 'node2', 'username': 'shirui', 'hostname': 'tycs.nsccty.com', 'port': 65091, 'key': None,
         'pkey': 'E:/downloads/work/HTCsys/public_key/tycs.nsccty.com_0113174144_rsa.txt'},
    ]
    m1 = Manager(workdict=workdict, CompNodesList=nlist,DataPadPath='E:\\downloads\\work\\HTCsys\\DataBase')
    m1.LogginProp()
    m1.ConnectNode('node1')
    launcher.SetClient(m1.ConnectedClient['node1'])
    #print(launcher.WorkNodeInfo)
    ret = launcher.RunWorkNode(block=0)
    print('cwd:',launcher.GetAbsPath())
    print(launcher.GetRunStat())
    launcher.STATE2RUNNING()
    time.sleep(5)
    launcher.KillRun()
    launcher.RunningDetect()
    print(launcher.stdout,ret)

