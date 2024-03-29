import math
import os.path
from typing import Dict, List
import re

import numpy as np

from utils.logger import logger
import json
import networkx as nx
from utils.Staff import WorkInfo


def FindRelation(edges):
    pk_ = []
    for e in edges:
        axes = ['->', '<-']
        for a in axes:
            if a in e:
                axe = a
        if axe == '->':
            parent, kid = e.split(axe)
        else:
            kid, parent = e.split(axe)
        pk_.append((int(parent),int(kid)))
    return pk_
class WorkNode():
    r'''
    ## all worknode should inherit this class
    '''
    def __init__(self,workinfo:Dict):
        r'''

        :param workinfo:
         workinfo = {1:{"state":"ALIVE","input":["some files",],"output":"some files","idx":1,"link":["1->2",],}

        '''
        workinfo = WorkInfo(workinfo=workinfo)
        self.name = workinfo.name
        self.cross_node_input = 0
        for attr in dir(workinfo):
            if attr.startswith('__'):
                continue
            self.__setattr__(attr,workinfo.__getattribute__(attr))
        if type(self.input) is WorkNode:
            self.cross_node_input = 1
            self.cross_node_idx = self.input.idx
            self.input = self.input.output

    def UnifyRunScript(self):
        logger.info('Unify the RunScript.')
        script = self.RunScript
        command_list = script.split('$$')
        matches = re.findall(r'\$\$(.*?)\$\$',script)
        for pos,i in enumerate(command_list):
            if i in matches:
                io_list = re.split(r'\[|\]',i)
                io_list = [ _ for _ in io_list if _ != '']
                command_list[pos] = self.__getattribute__(io_list[0])[int(io_list[1])]
        script = ''.join(command_list)
        #script = f'mkdir {self.cwd}; cd {self.cwd}; ' + script
        logger.info(f'Unify the RunScript [{self.RunScript}] to [{script}].')
        self.RunScript = script

    def AddRunScript(self,script:str,shell='bash',spec: Dict= {}) -> str:
        r'''
        :param script: script should specific the relationship among the command, input and output, use $$ symbol to specific the files
        :param spec: Dict to special option, such as command work directory cwd
        ## EXAMPLE:
               uniform_script = "command $$input[i]$$ command $$input[j]$$"
               self.input = ['ini.tpr',]
               self.output = ['out.gro',]
               example_script = "gmx mdrun -deffnm $$input[0]$$ -v -c $$output[0]$$ -ntmpi 1 -ntomp 12 -gpu_id 0 "
        :param shell: 'bash' is default
        :return: script
        '''
        if shell == 'bash':
            logger.info('Use bash shell script.')
            command_list = script.split('$$')
            matches = re.findall(r'\$\$(.*?)\$\$',script)
            for pos,i in enumerate(command_list):
                if i in matches:
                    io_list = re.split(r'\[|\]',i)
                    io_list = [ _ for _ in io_list if _ != '']
                    command_list[pos] = self.__getattribute__(io_list[0])[int(io_list[1])]
            if spec.get('workdir') is not None:
                workdir = spec['workdir']
                chdir = [f'cd {workdir}; ']
                command_list = chdir + command_list
            ## use spec input output
            if spec.get('input') is not None:
                for pos, i in enumerate(command_list):
                    if i in matches:
                        io_list = re.split(r'\[|\]', i)
                        io_list = [_ for _ in io_list if _ != '']
                        command_list[pos] = spec['input'][int(io_list[1])]
            elif spec.get('output') is not None:
                for pos, i in enumerate(command_list):
                    if i in matches:
                        io_list = re.split(r'\[|\]', i)
                        io_list = [_ for _ in io_list if _ != '']
                        command_list[pos] = spec['output'][int(io_list[1])]

            script = ''.join(command_list)
            if self.RunScript == '':
                self.RunScript = script
            else:
                self.RunScript = self.RunScript + '; ' + script
            logger.info(f'Work Named {self.name} Add RunScript: [{script}] to [{self.RunScript}].')
            return script

    def CleanRunScript(self):
        self.RunScript = ''
        logger.info(f'Clean All RunScript.')
        return self.RunScript

    def __repr__(self):
        workstat = {'name':self.name,
                    'idx':self.idx,
                    'state':self.state,
                    'input':self.input,
                    'output':self.output,
                    'RunScript':self.RunScript,
                    'link':self.link,
                    'pid_in_CNode':self.pid_in_CNode,
                    'cwd':self.cwd,
                    'cross_node_input':self.cross_node_input
                    }
        srepr = json.dumps(workstat,indent=4)
        return srepr
class WorkFlow():
    def __init__(self,workgraph:nx.DiGraph=None):
        r'''

        :param workgraph: DAG, Directed Acyclic Graph, specific the WorkFlowGraph, Generated by Manager.
        '''
        self.WorkGraph = workgraph
    def is_initialized(self):
        if self.WorkGraph is None:
            logger.error('FATAL ERROR WorkGraph is not initialized.')
            return False
        else:
            return True

    def InitializedFromNodeslist(self,nodeslist: List[WorkNode,]):
        self.WorkGraph = nx.DiGraph()
        for n in nodeslist:
            self.WorkGraph.add_node(n.idx,WorkNode=n)
            edges = n.link
            pk_ = FindRelation(edges)
            for pk in pk_:
                self.WorkGraph.add_edge(pk[0],pk[1])
        return

    def __repr__(self):
        workflowstat = {}
        for n in self.WorkGraph.nodes:
            workflowstat[n] = {'name':self.WorkGraph.nodes[n]['WorkNode'].name,
                               'idx':self.WorkGraph.nodes[n]['WorkNode'].idx,
                               'state':self.WorkGraph.nodes[n]['WorkNode'].state,
                               'input':self.WorkGraph.nodes[n]['WorkNode'].input,
                               'output':self.WorkGraph.nodes[n]['WorkNode'].output,
                               'RunScript':self.WorkGraph.nodes[n]['WorkNode'].RunScript,
                               'link':self.WorkGraph.nodes[n]['WorkNode'].link,
                               'pid_in_CNode':self.WorkGraph.nodes[n]['WorkNode'].pid_in_CNode,
                               'cwd':self.WorkGraph.nodes[n]['WorkNode'].cwd,}
        srepr = json.dumps(workflowstat,indent=4)
        return srepr

ncolorset = {
    'ALIVE':'grey',
    'READY':'purple',
    'RUNNING':'skyblue',
    'COMPLETE':'green',
    'DEAD':'red',
    'ERROR':'red'
}

def WorkFlowTo2DGraph(workflow:WorkFlow,filename='workflow.png',to_dir='.',pos={}):
    import matplotlib.pyplot as plt
    G = workflow.WorkGraph
    nodes = G.nodes
    n_nodes = len(nodes)
    dn = math.ceil(n_nodes**0.5)
    if pos == {}:
        for n in nodes:
            pos[n] = np.array([n%dn,int(n/dn)])
        #pos = nx.spring_layout(G,k=10,iterations=10,pos=pos)
        pos = nx.spring_layout(G)
    #print(pos)
    labels = {node: f'{node}-{G.nodes[node]["WorkNode"].state}' for node in G.nodes}
    nodecolor = []
    for n in G.nodes:
        nodecolor.append(ncolorset[G.nodes[n]['WorkNode'].state])
    nx.draw(G, pos=pos, arrows=True, node_size=1000, width=2, node_color=nodecolor,arrowstyle='->',alpha=0.7)
    nx.draw_networkx_labels(G, pos, labels=labels, font_color='blue',font_weight='bold',font_size=8)
    plt.savefig(os.path.join(to_dir,filename))
    plt.clf()
    #plt.show()
    return

class WorkFlowDataBase():
    def __init__(self ,workflow : nx.Graph):
        self.workflow = WorkFlow(workflow)

    def dump(self,filename='database',to_dir=None,pos={}):
        r'''
        dump all data to json, actually need to have log
        :return:
        '''
        if to_dir is None:
            to_dir='.'
        f = open(os.path.join(to_dir,filename+'.info'),'w')
        f.write(repr(self.workflow))
        f.close()
        WorkFlowTo2DGraph(self.workflow,filename=filename+'.png',to_dir=to_dir,pos=pos)
        return repr(self.workflow)


