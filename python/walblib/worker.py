import datetime
import yaml
from walblib import *

def parseFLAG(s):
    verify_type(s, str)
    if s == '0':
        return False
    if s == '1':
        return True
    raise Exception('parseFLAG:bad s', s)

def parseSuffix(s, suf):
    if type(s) == int:
        n = s
    else:
        verify_type(s, str)
        verify_type(suf, dict)
        suffix = 1
        c = s[-1]
        if c in suf:
            suffix = suf[c]
            s = s[0:-1]
        n = int(s)
        n *= suffix
    if n < 0:
        raise Exception('parseSuffix:negative value', s)
    return n

def parsePERIOD(s):
    """
        digits suffix
        digits = [0-9]+
        suffix = (m|h|d)
        or
        hh:mm:ss
    """
    if type(s) == str:
        v = s.split(':')
        if len(v) == 3:
            return datetime.timedelta(hours=int(v[0]), minutes=int(v[1]), seconds=int(v[2]))
    v = parseSuffix(s, {'m':60, 'h':3600, 'd':86400})
    return datetime.timedelta(seconds=v)

def parseSIZE_UNIT(s):
    """
        digits suffix
        suffix = (K|M|G)
    """
    return parseSuffix(s, {'K':1024, 'M':1024 * 1024, 'G':1024 * 1024 * 1024})

def parsePositive(d):
    verify_type(d, int)
    if d < 0:
        raise Exception('parsePositive:negative', d)
    return d

def parsePort(d):
    verify_type(d, int)
    if d < 0 or d > 65535:
        raise Exception('parsePort', d)
    return d

def timedelta2secondStr(ts):
    return str(int(ts.total_seconds()))

def formatIndent(d, indent):
    s = ''
    sp = ' ' * indent
    n = len(d)
    i = 0
    for (k, v) in d:
        s += sp
        s += k + ': '
        if isinstance(v, datetime.timedelta):
            s += timedelta2secondStr(v)
        elif v is not None:
            s += str(v)
        if i < n - 1:
            s += '\n'
        i += 1
    return s

def setValIfExist(obj, d, k, pred):
    if d.has_key(k):
        # obj.k = pred(d[k])
        setattr(obj, k, pred(d[k]))

def identity(x):
    return x

class GeneralConfig(object):
    def __init__(self):
        self.addr = ''
        self.port = 0
        self.walbc_path = ''
        self.max_task = 1
        self.max_replication_task = 1
        self.kick_interval = 1

    def set(self, d):
        verify_type(d, dict)
        tbl = {
            'addr': identity,
            'port': parsePort,
            'walbc_path': identity,
            'max_task': parsePositive,
            'max_replication_task': parsePositive,
            'kick_interval': parsePositive
        }
        for (k, pred) in tbl.items():
            setValIfExist(self, d, k, pred)

    def verify(self):
        if self.addr == '':
            raise Exception('GeneralConfig addr is not set')
        if self.port == 0:
            raise Exception('GeneralConfig port is not set')
        if self.walbc_path == '':
            raise Exception('GeneralConfig walbc_path is not set')
        if not os.path.exists(self.walbc_path):
            raise Exception('walbc_path is not found', self.walbc_path)

    def __str__(self, indent=2):
        d = [
            ('addr', self.addr),
            ('port', self.port),
            ('walbc_path', self.walbc_path),
            ('max_task', self.max_task),
            ('max_replication_task', self.max_replication_task),
            ('kick_interval', self.kick_interval),
        ]
        return formatIndent(d, indent)

class ApplyConfig(object):
    def __init__(self):
        self.keep_period = datetime.timedelta()
        self.interval = datetime.timedelta(days=1)
        self.time_window = (0, 0)
    def set(self, d):
        verify_type(d, dict)
        setValIfExist(self, d, 'keep_period', parsePERIOD)
        setValIfExist(self, d, 'interval', parsePERIOD)

    def verify(self):
        if self.keep_period == datetime.timedelta():
            raise Exception('ApplyConfig keep_period is not set')

    def __str__(self, indent=2):
        d = [
            ('keep_period', self.keep_period),
            ('interval', self.interval),
        ]
        return formatIndent(d, indent)

class MergeConfig(object):
    def __init__(self):
        self.interval = datetime.timedelta()
        self.max_nr = UINT64_MAX
        self.max_size = UINT64_MAX
        self.threshold_nr = UINT64_MAX
    def set(self, d):
        verify_type(d, dict)
        tbl = {
            'interval': parsePERIOD,
            'max_nr': parsePositive,
            'max_size': parseSIZE_UNIT,
            'threshold_nr': parsePositive,
        }
        for (k, pred) in tbl.items():
            setValIfExist(self, d, k, pred)

    def verify(self):
        if self.interval == datetime.timedelta():
            raise Exception('MergeConfig interval is not set')

    def __str__(self, indent=2):
        d = [
            ('interval', self.interval),
            ('max_nr', self.max_nr),
            ('max_size', self.max_size),
            ('threshold_nr', self.threshold_nr),
        ]
        return formatIndent(d, indent)

class ReplServerConfig(object):
    def __init__(self):
        self.name = ''
        self.addr = ''
        self.port = 0
        self.interval = datetime.timedelta()
        self.compress = CompressOpt()
        self.max_merge_size = '1G'
        self.max_send_size = 0
        self.bulk_size = '64K'
        self.log_name = ''
        self.enabled = True

    def set(self, name, d):
        verify_type(name, str)
        verify_type(d, dict)
        self.name = name
        tbl = {
            'addr':identity,
            'port': parsePort,
            'interval': parsePERIOD,
            'max_merge_size': str,
            'max_send_size': parseSIZE_UNIT,
            'bulk_size': str,
            'log_name': identity,
            'enabled': identity,
        }
        for (k, pred) in tbl.items():
            setValIfExist(self, d, k, pred)
        if d.has_key('compress'):
            self.compress.parse(d['compress'])
    def verify(self):
        if not self.enabled:
            return
        if self.addr == '':
            raise Exception('ReplServerConfig addr is not set')
        if self.port == 0:
            raise Exception('ReplServerConfig port is not set')
        if self.interval == datetime.timedelta():
            raise Exception('ReplServerConfig interval is not set')
        verify_type(self.addr, str)
        verify_type(self.log_name, str)
        verify_type(self.enabled, bool)

    def __str__(self, indent=2):
        d = [
            ('addr', self.addr),
            ('port', self.port),
            ('interval', self.interval),
            ('compress', self.compress),
            ('max_merge_size', self.max_merge_size),
            ('max_send_size', self.max_send_size),
            ('bulk_size', self.bulk_size),
            ('log_name', self.log_name),
            ('enabled', self.enabled),
        ]
        return formatIndent(d, indent)
    def getServerConnectionParam(self):
        return ServerConnectionParam(self.name, self.addr, self.port, K_ARCHIVE)

class ReplConfig(object):
    def __init__(self):
        self.servers = {}
        self.disabled_volumes = []
    def set(self, d):
        verify_type(d, dict)
        if d.has_key('servers'):
            ss = d['servers']
            for (name, v) in ss.items():
                if self.servers.has_key(name):
                    self.servers[name].set(name, v)
                else:
                    rs = ReplServerConfig()
                    rs.set(name, v)
                    self.servers[name] = rs
        name = 'disabled_volumes'
        if d.has_key(name) and d[name] is not None:
            verify_type(d[name], list, str)
            self.disabled_volumes = d[name]
    def verify(self):
        for rs in self.servers.values():
            rs.verify()
        if self.disabled_volumes:
            verify_type(self.disabled_volumes, list, str)

    def __str__(self):
        indent = 2
        n = len(self.servers)
        s = ' ' * indent + 'servers:\n'
        for (name, rs) in self.servers.items():
            s += ' ' * indent * 2 + name + ':\n'
            s += rs.__str__(indent * 3) + '\n'
        s += ' ' * indent + 'disabled_volumes:\n'
        if self.disabled_volumes:
            n = len(self.disabled_volumes)
            for i in xrange(n):
                s += ' ' * indent * 2 + '- ' + self.disabled_volumes[i]
                if i < n - 1:
                    s += '\n'
        return s

    def getEnabledList(self):
        rsL = []
        for rs in self.servers.values():
            if rs.enabled:
                rsL.append(rs)
        return rsL


class Config(object):
    def __init__(self, d=None):
        self.general = GeneralConfig()
        self.apply_ = ApplyConfig()
        self.merge = MergeConfig()
        self.repl = ReplConfig()
        if d is not None:
            self.set(d)

    def set(self, d):
        verify_type(d, dict)
        if d.has_key('general'):
            self.general.set(d['general'])
        if d.has_key('apply'):
            self.apply_.set(d['apply'])
        if d.has_key('merge'):
            self.merge.set(d['merge'])
        if d.has_key('repl'):
            self.repl.set(d['repl'])

    def verify(self):
        self.general.verify()
        self.apply_.verify()
        self.merge.verify()
        self.repl.verify()

    def setStr(self, s):
        verify_type(s, str)
        d = yaml.load(s)
        self.set(d)

    def load(self, configName):
        verify_type(configName, str)
        s = ''
        if configName == '-':
                s = sys.stdin.read()
        else:
            with open(configName) as f:
                s = f.read()
        self.setStr(s)

    def __str__(self):
        s = "general:\n"
        s += str(self.general) + '\n'
        s += "apply:\n"
        s += str(self.apply_) + '\n'
        s += "merge:\n"
        s += str(self.merge) + '\n'
        s += 'repl:\n'
        s += str(self.repl)
        return s
