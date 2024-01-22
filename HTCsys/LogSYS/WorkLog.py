import os
import time
from utils.logger import logger
from utils.Staff import Logger,WorkInfo,CompNode
import paramiko
import re
import inspect
from typing import List

class Analyze():
    def __init__(self):
        pass

    def set_period(self,period):
        r'''

        :param period: period = delay + set_period, often delay = 10 s
        :return:
        '''
        self.period = period
        return

    def SetupLog(self):
        self.callback()
        logger.info(f'log runscript: {self.Log.WorkNodeInfo.RunScript}')
        info = self.Log.CompNodeInfo
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        hostname, username,port, key, pkey = (
        info.hostname, info.username, info.port, info.key, info.pkey)
        if info.loggin == 'key':
            try:
                client.connect(hostname=hostname, username=username, port=port, password=key)
                stdin, stdout, stderr = client.exec_command(self.Log.WorkNodeInfo.RunScript)
            except paramiko.ssh_exception.AuthenticationException as e:
                logger.error(f'FATAL ERROR: {e}')
        elif info.loggin == 'pkey':
            try:
                private_key = paramiko.RSAKey.from_private_key_file(pkey)
                client.connect(hostname=hostname, username=username, port=port, pkey=private_key)
                stdin, stdout, stderr = client.exec_command(self.Log.WorkNodeInfo.RunScript)
            except paramiko.ssh_exception.AuthenticationException as e:
                logger.error(f'FATAL ERROR in LogSYS: {e}')
        else:
            logger.error(f'Failed to connect to node {info.nodename}. Please provide your key or public key to {info.nodename}.')
        workdone = 0
        while not workdone:
            logger.info('log from pulse.')
            self.callback()
            stdin, stdout, stderr = client.exec_command(self.Log.WorkNodeInfo.RunScript)
            while not stdout.channel.exit_status_ready():
                time.sleep(0.1)
            #time.sleep(2)
            self.fetch_log()
            stdin, stdout, stderr = client.exec_command(self.FetchScript)
            while not stdout.channel.exit_status_ready():
                time.sleep(0.1)
            stdout = stdout.read(1024).decode()
            time.sleep(1)
            logger.info(f'log information : {stdout}')
            time.sleep(self.period)
            if stdout == '':
                workdone = 1
                logger.info('log information : log finished.')
        return



class Pulse(Analyze):
    def __init__(self,Log:Logger = None):
        Analyze.__init__(self)
        self.name = 'pulse'
        self.Log = Log
        if Log is None:
            logger.error('no Logger provide.')
            raise
        self.Log = Log
        self.period = self.Log.period
        logger.info(f'This is a {self.name} logger to detect the run work')

    def callback(self,mode='cluster',func=False):
        ## this is a pulse callback to create a log information
        ## define your specific call back to get your custom log information, perhaps some information analyze by a function
        r'''

        :param input:
        :param mode: default 'cluster', use 'sbatch' as well
        :return:
        '''
        output = ['log.txt', ]
        self.input = self.Log.WorkNodeInfo.pid_in_CNode
        self.output = output
        self.Log.input = self.Log.WorkNodeInfo.pid_in_CNode
        self.Log.output = output
        if mode == 'cluster':
            RunScript = f'cd {self.Log.WorkNodeInfo.cwd}; ps -ef | grep {self.Log.WorkNodeInfo.pid_in_CNode} | grep -v grep > {output[0]}'
            self.Log.WorkNodeInfo.RunScript = RunScript
        if mode == 'sabtch':
            RunScript = f'cd {self.Log.WorkNodeInfo.cwd};scontrol show job {self.Log.WorkNodeInfo.pid_in_CNode} > {output[0]}'
            self.Log.WorkNodeInfo.RunScript = RunScript
        return

    def fetch_log(self):
        FetchScript = f'cd {self.Log.WorkNodeInfo.cwd}; '
        FetchScript = FetchScript + ' ;'.join([f'cat {o}' for o in self.output])
        self.FetchScript = FetchScript
        #print(FetchScript)
        return

def _func(input,output):
    r'''
    use input and output list to define your function
    :param input:
    :param output:
    :return:
    '''
    os.system('echo hello world!')
    return

class customlog(Analyze):
    def __init__(self,name,Log:Logger = None):
        Analyze.__init__(self)
        self.name = name
        logger.info(f'This is a {self.name} logger to detect the run work')
        if Log is None:
            logger.error('no Logger provide.')
            raise
        self.Log = Log
        self.period = self.Log.period

    def callback(self,input: List = [],output: List = [],RunScript: str ='',func: callable =None):
        r'''

        :param input:
        :param output:
        :param RunScript:
        :param func:
        :return:
        '''
        self.output = output
        self.input = input
        if func is None:
            io_spec = {'input': input, 'output': output}
            script = RunScript
            command_list = script.split('$$')
            matches = re.findall(r'\$\$(.*?)\$\$', script)
            for pos, i in enumerate(command_list):
                if i in matches:
                    io_list = re.split(r'\[|\]', i)
                    io_list = [_ for _ in io_list if _ != '']
                    command_list[pos] = io_spec.__getattribute__(io_list[0])[int(io_list[1])]
            script = ''.join(command_list)
            self.RunScript = script
            return
        if func is not None:
            pyname = func.__name__
            source_code = inspect.getsource(func)
            pattern = r'{}\(.*?\)'.format(pyname)
            match = re.findall(pattern,source_code)
            matches = re.split(r'[\(\)]',match[0])
            paras = matches[1].split(',')
            for i,p in enumerate(paras):
                if p == 'input':
                    input_i = i
                if p == 'output':
                    output_i = i
            n_input = len(input)
            n_output = len(output)
            pyinput_s = ''.join([f'argv[{i+1}],' for i in range(n_input)])
            pyoutput_s = ''.join([f'argv[{i+1}],' for i in range(n_input,n_input+n_output)])
            input_s = ''.join([f'{i} ' for i in input])
            output_s = ''.join([f'{i} ' for i in output])
            addition_code = 'from sys import argv\n' + f'input = [{pyinput_s}]\noutput = [{pyoutput_s}]\n'
            source_code = addition_code + source_code + f'\n{match[0]}\n'
            RunScript = f'cd {self.Log.WorkNodeInfo.cwd}; echo "{source_code}" > {pyname}.py; python {pyname}.py {input_s} {output_s}'
            self.RunScript = RunScript
            return

if __name__ == '__main__':
    from utils.Staff import WorkInfo,CompNode
    Log = Logger(NodeInfo={'WorkNode':{"state":"ALIVE","input":["test",'0'],"output":['test.gro'],"idx":1,"link":["1->2",],
                    "RunScript":'gmx mdrun -deffnm $$input[0]$$ -v -c $$output[0]$$ -ntmpi 1 -ntomp 12 -gpu_id $$input[1]$$',
                    "pid_in_CNode":39399,"cwd":'/home/shirui/lmy/test/Run_Data/trial/test','name':1},
                           'CompNode':{'nodename':'node1','username':'shirui','hostname':'10.10.2.126','port':22,'key':'tony9527'}})
    pulselog = Pulse(Log=Log)
    pulselog.set_period(2)
    pulselog.SetupLog()