## Walb-toolsチュートリアル

このチュートリアルはWalb-tools(以下tools)の使い方を説明します。
詳細は[README](README.md)を参照してください。

* Walb概要
Walb自体の概要は[Walb概要pptx](walb-is-hard.pptx)を参照してください。

* Walbシステム
  * storage : バックアップ対象となるデーモン。Walbドライバが載っていてディスクへのの書き込みに対してlogを生成する。
  * proxy : storageからlogを吸い出してwdiff形式に変換してarchiveに転送する。
  * archive : wdiffを貯蔵する。wdiffをあるsnapshotに適用して好きな時刻のsnapshotを作成する。

* Walb最小構成
  * PC2台
    * pc1 : バックアップ対象
    * pc2 : バックアップしたものをおくところ
  * デーモン : storage, proxy, archiveが一つずつ存在する。

* システム設計をする。
  * サービス構成
    * s0(storage) : pc1のport 10000
    * p0(proxy) : pc2のport 10100
    * a0(archive) : pc2のport 10200
  * ディスク構成
    * pc1に/dev/data/log, /dev/data/dataという名前のLVMを作る。
      * dataはそのサーバが実際に使う領域。サイズはたとえば128GiBとか。
      * logはwalbがバックアップのための情報を書き込む領域。たとえば4GiBとか。
    * pc2に/var/walb/{p0,a0}というディレクトリを作る。
      * pc2の/var/walb/p0はproxyデーモンが利用するディレクトリ。
      * pc2の/var/walb/a0はarchiveデーモンが利用するディレクトリ。
    * 更にpc2にpc1のdataを復元する領域data2を作る。少なくともpc1のdataより大きい空き容量が必要。
    * [図pptx](tutorial-fig.pptx)参照

* システム構築手順
  * walb-toolsをインストールする。
  * config.pyを作る。例 :
```
#!/usr/bin/env python

import sys
DIR='<walb-toolsのディレクトリ>/'
sys.path.append(DIR + 'python/walb/')
from walb import *

binDir = DIR + 'binsrc/'
wdevcPath = binDir + 'wdevc'
walbcPath = binDir + 'walbc'

def dataPath(s):
    return '/var/walb/%s/' % s

def logPath(s):
    return '/var/walb/%s.log' % s

s0 = Server('s0', 'pc1', 10000, K_STORAGE, binDir, dataPath('s0'), logPath('s0'))
p0 = Server('p0', 'pc2', 10100, K_PROXY, binDir, dataPath('p0'), logPath('p0'))
a0 = Server('a0', 'pc2', 10200, K_ARCHIVE, binDir, dataPath('a0'), logPath('a0'), 'data2')

sLayout = ServerLayout([s0], [p0], [a0])
walbc = Controller(walbcPath, sLayout, isDebug=False)

runCommand = walbc.get_run_remote_command(s0)
wdev0 = Device(0, '/dev/data/log', '/dev/data/data', wdevcPath, runCommand)

VOL = 'vol0'
```
