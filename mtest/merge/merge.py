from config import *
import random
import sys

md0 = sys.argv[1]

def getTwo(ls):
    n = len(ls)
    r1 = random.randint(0, n - 2)
    r2 = random.randint(r1 + 1, n - 1)
    return (ls[r1], ls[r2])

minls = walbc.get_restorable(a0, VOL)
gid = minls[-1]
while True:
    ls = walbc.get_restorable(a0, VOL, 'all')
#   if len(ls) < len(minls) * 2:
    if len(ls) < 100:
        break
    (d1, d2) = getTwo(ls)
    print "merge", d1, d2, len(ls)
    try:
        walbc.merge_diff(a0, VOL, d1, d2)
        print "done"
    except Exception, e:
        print "ignore:", e
        pass

walbc.restore(a0, VOL, gid)
path = walbc.get_restored_path(a0, VOL, gid)
md1 = walbc.run_remote_command(a0, ['/usr/bin/sha1sum', path], timeoutS=1000).split(' ')[0]
if md0 == md1:
    print "OK"
    exit(0)
else:
    print "ERR:md"
    print md0
    print md1
    exit(1)
