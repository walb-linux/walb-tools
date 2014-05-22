import os
from walb_cmd import *

s0 = Server('s0', '9000', None)
s1 = Server('s1', '9001', None)
s2 = Server('s2', '9002', None)
p0 = Server('p0', '9100', None)
p1 = Server('p1', '9101', None)
p2 = Server('p2', '9102', None)
a0 = Server('a0', '9200', None)
a1 = Server('a1', '9201', 'vg0')
a2 = Server('a2', '9202', 'vg1')

config = Config(False, os.getcwd(), os.getcwd(), [s0, s1], [p0, p1], [a0, a1])
setConfig(config)

def main():
    runServerDaemon(s0)
    #runServerDaemon(s1)
    #runServerDaemon(p0)
    #runServerDaemon(p1)
    #hostType = runCtrl("s0", ["host-type"])
    #shutdown("s0")

if __name__ == "__main__":
    main()
