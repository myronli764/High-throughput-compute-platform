import paramiko

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
    def __init__(self,worknodeinfo: Dict,compnode:Dict):
        r'''
        ### must remove the backspace in the right of the RunScript!!

        :param worknodeinfo: {"state":"ALIVE","input":["some files",],"output":"some files","idx":1,"link":["1->2",],"RunScript":'xxx',"pid_in_CNode":0,"cwd":'/home'}
        :param compnode: {'nodename':'node1','username':'shirui','hostname':'10.10.2.126','port':22,'key':'tony9527','pkey':None}
        '''
        self.WorkNodeInfo = worknodeinfo
        for i,info in compnode.items():
            self.__setattr__(i,info)
        if self.WorkNodeInfo is None:
            logger.error(f'there is no work in this compnode')
        if self.nodename is None:
            logger.error(f'WorkNode {self.WorkNodeInfo["name"]} failed to allocate CompNode.')
    def STATEToRUNING(self):
        ## return the state, but set by Manager
        return 'RUNNING'

    def SetClient(self,client:paramiko.SSHClient):
        self.Client = client
    def GetAbsPath(self,slurm=False,cluster=True):
        r'''
        only linux system, specific the infrastructure.
        :param slurm:
        :param cluster:
        :return:
        '''

        if cluster :
            RunScript = self.WorkNodeInfo["RunScript"]
            pwd = '/usr/sbin/lsof -p `ps -ef | grep "%s" | grep -v grep | awk \'{print $2}\'` | grep cwd | awk \'{print $9}\'' % (RunScript)
            stdin, stdout, stderr = self.Client.exec_command(pwd)
            cwd = stdout.read().decode()
            return cwd
        if slurm:
            pwd = f'scontrol show job {self.WorkNodeInfo["pid_in_CNode"]} | grep WorkDir'
            stdin, stdout, stderr = self.Client.exec_command(pwd)
            cwd = stdout.read().decode()
            return [var.read().decode() for var in [stdin, stdout, stderr]]


    def GetCommandInfo(self,slurm=False,cluster=True):
        if self.WorkNodeInfo['pid_in_CNode'] == 0:
            logger.error(f'Have not run the command {self.WorkNodeInfo["pid_in_CNode"]}')
            return 0
        if cluster:
            exe_info = f'lsos -p {self.WorkNodeInfo["pid_in_CNode"]} | grep cwd'
            stdin, stdout, stderr = self.Client.exec_command(exe_info)
            info = stdout.read().decode()
            if info == '':
                info = 'Not on running'
            else:
                info = 'On running'
            return info
        if slurm:
            exe_info = f'squeue | grep {self.WorkNodeInfo["pid_in_CNode"]}'
            stdin, stdout, stderr = self.Client.exec_command(exe_info)
            info = stdout.read().decode()
            if info == '':
                info = 'Not on queue'
            else:
                info = 'On queue'
            return info

    def RunWorkNode(self,cluster=True,slurm=False,block=1):
        if cluster:

            channel1 = self.Client.get_transport().open_session()
            channel1.setblocking(block)
            channel1.exec_command(self.WorkNodeInfo["RunScript"])
            self.RunChannel = channel1
            pid = "ps -ef | grep '%s' | grep -v grep | awk '{print $2}'" % (self.WorkNodeInfo["RunScript"])
            channel = self.Client.get_transport().open_session()
            channel.setblocking(block)
            channel.exec_command(pid)
            time.sleep(1)
            pid,__ = (channel.recv(1024).decode(), channel.recv_stderr(1024).decode())
            ret = pid

            return ret
        if slurm:
            channel = self.Client.get_transport().open_session()
            channel.setblocking(block)
            stdin, stdout, stderr = channel.exec_command(self.WorkNodeInfo["RunScript"])
            _, pid, __ = channel.exec_command('%s | awk \'{print $4}\''%(stdout.read().decode()))
            ret = [var.read().decode() for var in [ stdout, stderr, pid]]
            channel.close()
            return ret

    def GetRunStat(self,slurm=False,cluster=True):
        stat = {}
        stat['state'] = self.WorkNodeInfo["state"]
        stat['pwd'] = self.GetAbsPath()
        stat['RunScript'] = self.WorkNodeInfo["RunScript"]
        if slurm is True:
            stat['squeue'] = self.GetCommandInfo()
        return stat
    def RunningDetect(self):
        while not self.RunChannel.exit_status_ready():
            time.sleep(1)
        self.stdout = self.RunChannel.recv(1024).decode()
        self.stderr = self.RunChannel.recv_stderr(1024).decode()
        return

if __name__ == '__main__':
    from ManSYS.Manager import Manager
    Cnode = {'nodename': 'node1', 'username': 'shirui', 'hostname': '10.10.2.126', 'port': 22, 'key': 'tony9527', 'pkey': None}
    ## 'gmx grompp -c lmy/test/Run_Data/ini.gro -f lmy/test/Run_Data/trial/trial.mdp -p lmy/test/Run_Data/topol.top -o lmy/test/Run_Data/trial/test -maxwarn 100'
    ## 'gmx mdrun -deffnm lmy/test/Run_Data/trial/test -v -c lmy/test/Run_Data/trial/test.gro -ntmpi 1 -ntomp 12 -gpu_id 3'
    Worknodeinfo = {"state":"ALIVE","input":["zxz",],"output":"some files","idx":1,"link":["1->2",],
                    "RunScript":'gmx mdrun -deffnm lmy/test/Run_Data/trial/test -v -c lmy/test/Run_Data/trial/test.gro -ntmpi 1 -ntomp 12 -gpu_id 3',
                    "pid_in_CNode":0,"cwd":'/home'}
    launcher = Launcher(worknodeinfo=Worknodeinfo,compnode=Cnode)
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
         'pkey': 'E:/downloads/work/HTCsys/public_key/tycs.nsccty.com_0108162129_rsa.txt'},
    ]
    m1 = Manager(workdict=workdict, CompNodesList=nlist)

    m1.LogginProp()
    m1.ConnectNode('node1')
    launcher.SetClient(m1.ConnectedClient['node1'])
    #print(launcher.WorkNodeInfo)
    ret = launcher.RunWorkNode(block=0)
    print('cwd:',launcher.GetAbsPath())
    launcher.RunningDetect()
    print(launcher.stdout,ret)

