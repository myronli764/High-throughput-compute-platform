import paramiko

from utils.logger import logger
from DataBase.database import WorkNode
from typing import Dict

import os
'''
nlist = [
         ## node1 =      {'nodename':'node1','username':'shirui','hostname':'10.10.2.126','port':22,'key':'tony9527','pkey':None},
         ## node2 =      {'nodename':'node2','username':'shirui','hostname':'tycs.nsccty.com','port':65091,'key':None,'pkey':'E:/downloads/work/HTCsys/public_key/tycs.nsccty.com_0108162129_rsa.txt'},
                ]
'''
class Launcher():
    ### do not sent the WorkNode to Launcher, just information is enough
    def __init__(self,worknode: WorkNode,compnode:Dict):
        self.WorkNode = worknode
        for i,info in compnode.items():
            self.__setattr__(i,info)
        if self.WorkNode is None:
            logger.error(f'there is no work in this compnode')
        if self.nodename is None:
            logger.error(f'WorkNode {self.WorkNode.name} failed to allocate CompNode.')
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
            RunScript = self.WorkNode.RunScript
            pwd = 'lsof -p `ps -ef | grep %s | grep -v grep | awk \'{print $2}\'` | grep cwd | awk \'{print $9}\'' % (RunScript)
            stdin, stdout, stderr = self.Client.exec_command(pwd)
            cwd = stdout.read().decode()
            return cwd
        if slurm:
            pwd = f'scontrol show job {self.WorkNode.pid_in_CNode} | grep WorkDir'
            stdin, stdout, stderr = self.Client.exec_command(pwd)
            cwd = stdout.read().decode()
            return cwd


    def GetCommandInfo(self,slurm=False,cluster=True):
        if self.WorkNode.pid_in_CNode == 0:
            logger.error(f'Have not run the command {self.WorkNode.RunScript}')
            return 0
        if cluster:
            exe_info = f'lsos -p {self.WorkNode.pid_in_CNode} | grep cwd'
            stdin, stdout, stderr = self.Client.exec_command(exe_info)
            info = stdout.read().decode()
            if info == '':
                info = 'Not on running'
            else:
                info = 'On running'
            return info
        if slurm:
            exe_info = f'squeue | grep {self.WorkNode.pid_in_CNode}'
            stdin, stdout, stderr = self.Client.exec_command(exe_info)
            info = stdout.read().decode()
            if info == '':
                info = 'Not on queue'
            else:
                info = 'On queue'
            return info

    def RunWorkNode(self):
        self.Client.exec_command(self.WorkNode.RunScript)
        return

    def GetRunStat(self,slurm=False,cluster=True):
        stat = {}
        stat['state'] = self.WorkNode.state
        stat['pwd'] = self.GetAbsPath()
        stat['RunScript'] = self.WorkNode.RunScript
        if slurm is True:
            stat['squeue'] = self.GetCommandInfo()
        return stat




