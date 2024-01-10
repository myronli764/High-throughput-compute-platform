import matplotlib.pyplot as plt
import numpy as np

rdf2 = np.loadtxt('rdfN2.txt').T
rdf3 = np.loadtxt('rdfN3.txt').T
rdf4 = np.loadtxt('rdfN4.txt').T
rdf5 = np.loadtxt('rdfN5.txt').T
'''
plt.plot(rdf2[0],rdf2[1],label='epsilon=2')
plt.plot(rdf3[0],rdf3[1],label='epsilon=3')
plt.plot(rdf4[0],rdf4[1],label='epsilon=4')
plt.plot(rdf5[0],rdf5[1],label='epsilon=5')
'''

msdn2 = np.loadtxt('msd_NP2.txt').T
msdn3 = np.loadtxt('msd_NP3.txt').T
msdn4 = np.loadtxt('msd_NP4.txt').T
msdn5 = np.loadtxt('msd_NP5.txt').T

msdc2 = np.loadtxt('msd_Chain2.txt').T
msdc3 = np.loadtxt('msd_Chain3.txt').T
msdc4 = np.loadtxt('msd_Chain4.txt').T
msdc5 = np.loadtxt('msd_Chain5.txt').T
'''
plt.plot(msdn2[0],msdn2[1],label='NP epsilon=2',ls='--')
plt.plot(msdn3[0],msdn3[1],label='NP epsilon=3',ls='--')
plt.plot(msdn4[0],msdn4[1],label='NP epsilon=4',ls='--')
plt.plot(msdn5[0],msdn5[1],label='NP epsilon=5',ls='--')

plt.plot(msdc2[0],msdc2[1],label='Chain epsilon=2')
plt.plot(msdc3[0],msdc3[1],label='Chain epsilon=3')
plt.plot(msdc4[0],msdc4[1],label='Chain epsilon=4')
plt.plot(msdc5[0],msdc5[1],label='Chain epsilon=5')
'''

x = np.linspace(0,20000*1000,20000)
d2 = np.load('mean_acf2.npz')
d3 = np.load('mean_acf3.npz')
d4 = np.load('mean_acf4.npz')
d5 = np.load('mean_acf5.npz')

print(d2['res'][::,0])

y2 = d2['res'][::,0]
y3 = d3['res'][::,0]
y4 = d4['res'][::,0]
y5 = d5['res'][::,0]


plt.plot(x,y2,label='p=1 epsilon=2')
plt.plot(x,y3,label='p=1 epsilon=3')
plt.plot(x,y4,label='p=1 epsilon=4')
plt.plot(x,y5,label='p=1 epsilon=5')



plt.xscale('log')
#plt.yscale('log')

plt.xlabel('t')
plt.ylabel('Rouse mode CF')
plt.legend()
plt.show()