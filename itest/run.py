import subprocess, shlex, sys, time

class ProcData(object):
    def __init__(self, proc, inFile, outFile, args):
        '''
        proc :: subprocess.Popen
        inFile :: file or None
        outFile :: file or None
        args :: [str] - command line arguments.
        '''
        self.proc = proc
        self.inFile = inFile
        self.outFile = outFile
        self.args = args

    def wait(self):
        '''
        return :: bool - True if exit code is 0.
        '''
        ret = self.proc.wait()
        for f in [self.inFile, self.outFile]:
            if f:
                f.close()
        return ret == 0


def run(args, block=True):
    '''
    args :: str
    block :: bool
    '''
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
    proc = ProcData(p, inFile, outFile, cmd)
    if not block:
        return proc
    ret = proc.wait()
    t1 = time.time()
    print 'exectime: ', t1 - t0
    if not ret:
        raise EnvironmentError(cmd)


def run_async(args):
    '''
    args :: [str]
    return :: ProcData
    '''
    return run(args, block=False)


def check_result(msg):
    print "TEST_SUCCESS", msg
