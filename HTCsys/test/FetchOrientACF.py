from ManSYS.Manager import Manager
from utils.Staff import *
from LogSYS.WorkLog import customlog
import numpy as np
import os

def ACF(input,ouput):
    import numpy as np
    import os
    nppos, npposAA, npposBB = input
    pos = np.load(nppos)
    posAA = np.load(npposAA)
    posBB = np.load(npposBB)
    cl = 16
    nframes = len(pos)
    t = np.arange(0,nframes) * 10
    ## pos AB
    posAB = pos.reshape(nframes,-1,16,3)
    rAB = np.diff(posAB,axis=2)
    rAB = rAB.reshape((nframes, -1, 3))
    acf_AB = np.mean(
        np.sum(((np.fft.ifft(np.abs(np.fft.fft(rAB, n=2 * nframes, axis=0) ** 2), n=2 * nframes, axis=0)).real)[:nframes],
               axis=-1), axis=-1) / np.arange(nframes, 0, -1)
    acf_AB =  acf_AB / np.sum(rAB ** 2, axis=-1).mean()
    ## pos AA
    posAA = posAA.reshape(nframes, -1, 8, 3)
    rAA = np.diff(posAA, axis=2)
    rAA = rAA.reshape((nframes, -1, 3))
    acf_AA = np.mean(
        np.sum(
            ((np.fft.ifft(np.abs(np.fft.fft(rAA, n=2 * nframes, axis=0) ** 2), n=2 * nframes, axis=0)).real)[:nframes],
            axis=-1), axis=-1) / np.arange(nframes, 0, -1)
    acf_AA = acf_AA / np.sum(rAA ** 2, axis=-1).mean()
    ## pos BB
    posBB = posBB.reshape(nframes, -1, 8, 3)
    rBB = np.diff(posBB, axis=2)
    rBB = rBB.reshape((nframes, -1, 3))
    filenameAB, ext = os.path.splitext(nppos)
    filenameAA, ext = os.path.splitext(npposAA)
    filenameBB, ext = os.path.splitext(npposBB)
    acf_BB = np.mean(
        np.sum(
            ((np.fft.ifft(np.abs(np.fft.fft(rBB, n=2 * nframes, axis=0) ** 2), n=2 * nframes, axis=0)).real)[:nframes],
            axis=-1), axis=-1) / np.arange(nframes, 0, -1)
    acf_AB = acf_BB / np.sum(rBB ** 2, axis=-1).mean()
    np.save(filenameAB + '_acf', np.vstack((t,acf_AB)).T)
    np.save(filenameAA + '_acf', np.vstack((t,acf_AA)).T)
    np.save(filenameBB + '_acf', np.vstack((t,acf_BB)).T)
    f = open('log.txt','w')
    f.write('')
    f.close()
    return

nodelist = []
workdict = {}
dirs = open('good_pi.txt','r').read().split(',')
wid = 0
tem = ['1000','900','800']

'''
## worknode to analyze
for i,d in enumerate(dirs[:1]):
    for c in range(3):
        workdict[wid] = {"state": "ALIVE", "input": ['/home1/shirui/PI/GAYchienLearning/aa_md_data/pi/ACF.py',f'{tem[c]}KAB.npy',f'{tem[c]}KAA.npy',f'{tem[c]}KBB.npy'],
                         "output": [],
                         "RunScript": 'python $$input[0]$$ $$input[1]$$ $$input[2]$$ $$input[3]$$ ', 'name': f'{d}_{tem[c]}', "idx": wid, "link": [],
                         'cwd':f'/home1/shirui/PI/GAYchienLearning/aa_md_data/pi/{d}/Run_Data/Run'}
        wid +=1
'''


##  analyzer
for i,d in enumerate(dirs[:1]):
    for c in range(3):
        workdict[wid] = {"state": "ALIVE", "input": ['ACF.py',f'{tem[c]}KAB.npy',f'{tem[c]}KAA.npy',f'{tem[c]}KBB.npy'],
                         "output": [],
                         "RunScript": ' sleep 15', 'name': f'{d}_{tem[c]}', "idx": wid, "link": [],
                         'cwd':f'~'}
        wid +=1

m = Manager(workdict=workdict,CompNodesList=nodelist,DataPadPath='E:/downloads/work/HTCsys/test/')
m.JSONToWorkFlow()
m.LogginProp()
m.ConnectNodeTest(close=False)
m.DataBase.dump()
for n in m.WorkFlow.nodes:
    name = m.WorkFlow.nodes[n]['WorkNode'].name
    m.set_analyze(n,f'{name}_acf_analyze')
m.RunWorkFlow()
