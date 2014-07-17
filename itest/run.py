import subprocess, shlex, sys, time

def run(args):
    print "args:", args
    cmd = shlex.split(args)
    inFile = None
    outFile = None
    if "<" in cmd:
        pos = cmd.index("<")
        inFile = open(cmd[pos + 1], "r")
        cmd[pos:pos+2] = []
    if ">" in cmd:
        pos = cmd.index(">")
        outFile = open(cmd[pos + 1], "w")
        cmd[pos:pos+2] = []

    t0 = time.time()
    p = subprocess.Popen(cmd, stdin=inFile, stdout=outFile, stderr=sys.stdout)
    ret = p.wait()
    t1 = time.time()
    print 'exectime: ', t1 - t0
    if inFile:
        inFile.close()
    if outFile:
        outFile.close()
    if ret:
        raise EnvironmentError(cmd)

def check_result(msg):
    print "TEST_SUCCESS", msg

