from ManSYS.Manager import Manager
from LogSYS.WorkLog import Pulse

workdict = {
        0:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'sh trial.sh', 'name': 'initial', "idx": 0, "link": ["0->1", ]},
        1:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 1, "idx": 1, "link": ["1->2", "1->6"]},
        2:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 2, "idx": 2, "link": ["2->3", ]},
        3:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 3, "idx": 3, "link": ["3->4", ]},
        4:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 4, "idx": 4, "link": ["4->5", ]},
        5:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 5, "idx": 5, "link": []},
        6:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 6, "idx": 6, "link": ["6->7", ]},
        7:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 7, "idx": 7, "link": ["7->8", "7->9"]},
        8:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 8, "idx": 8, "link": []},
        9:  {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 9, "idx": 9, "link": ["9->10", ]},
        10: {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 10, "idx": 10, "link": []},
        11: {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 11, "idx": 11, "link": ["11->12", ]},
        12: {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 12, "idx": 12, "link": ["12->13", ]},
        13: {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 13, "idx": 13, "link": ["13->7","13->14"]},
        14: {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 14, "idx": 14,"link": ["14->15"]},
        15: {"state": "ALIVE", "input": ["~", ], "output": ["~", ], "RunScript": 'mkdir htc_test; cd htc_test; sleep 1;ls ../*', 'name': 15, "idx": 15,"link": []},

    }

nodelist = [
    {'nodename': 'node1' , 'nodeidx': 0, 'username': 'shirui', 'hostname': '10.10.2.126', 'port': 22, 'key': 'tony9527','pkey': None},
    #{'nodename': 'node2' , 'nodeidx': 1, 'username': 'shirui', 'hostname': '10.10.2.126', 'port': 22, 'key': 'tony9527','pkey': None},
    #{'nodename': 'node3' , 'nodeidx': 2, 'username': 'shirui', 'hostname': '10.10.2.126', 'port': 22, 'key': 'tony9527','pkey': None},
    #{'nodename': 'node4' , 'nodeidx': 3, 'username': 'shirui', 'hostname': '10.10.2.126', 'port': 22, 'key': 'tony9527','pkey': None},
    #{'nodename': 'node5' , 'nodeidx': 4, 'username': 'shirui', 'hostname': '10.10.2.125', 'port': 22, 'key': 'tony9527','pkey': None},
    #{'nodename': 'node6' , 'nodeidx': 5, 'username': 'shirui', 'hostname': '10.10.2.125', 'port': 22, 'key': 'tony9527','pkey': None},
    #{'nodename': 'node5' , 'nodeidx': 6, 'username': 'shirui', 'hostname': '10.10.2.125', 'port': 22, 'key': 'tony9527','pkey': None},
    #{'nodename': 'node6' , 'nodeidx': 7, 'username': 'shirui', 'hostname': '10.10.2.125', 'port': 22, 'key': 'tony9527','pkey': None},
    ]

presqm = Manager(name='presq',workdict=workdict,CompNodesList=nodelist)