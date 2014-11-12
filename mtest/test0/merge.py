
execfile('walb-config.py')
gidL = walbc.get_restorable(a0, VOL, 'all')
print len(gidL)

ret = []
while len(gidL) >= 3:
    r = gidL[0:3]
    ret.append(r)
    gidL.pop(0)
    gidL.pop(0)
    gB = r[0]
    gE = r[-1]
    print 'merge', gB, gE
    walbc.merge(a0, VOL, gB, gE)
    
