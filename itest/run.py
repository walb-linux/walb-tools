import subprocess, shlex, sys

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

    p = subprocess.Popen(cmd, stdin=inFile, stdout=outFile, stderr=sys.stdout)
    ret = p.wait()
    if inFile:
        inFile.close()
    if outFile:
        outFile.close()
    if ret:
        raise EnvironmentError(cmd)

def check_result(msg):
    print "TEST_SUCCESS", msg

