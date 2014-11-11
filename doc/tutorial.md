# WalB-toolsチュートリアル

このチュートリアルはWalB-tools(以下tools)の使い方を説明します。
詳細は[README](README.md)を参照してください。

## WalB概要
WalB自体の概要は[WalB概要pptx](https://github.dev.cybozu.co.jp/herumi/walb-tools/raw/master/doc/walb-is-hard.pptx)を参照してください。

* WalBシステム
  * storage : バックアップ対象となるサーバ。WalBドライバが載っていてディスクへのの書き込みに対してlogを生成する。
  * proxy : storageからlogを吸い出してwdiff形式に変換してarchiveに転送する。
  * archive : wdiffを貯蔵する。wdiffをあるsnapshotに適用して好きな時刻のsnapshotを作成する。

* 最小構成で試してみる
  * PC1台でwalb-storage, walb-proxy, walb-archiveを動かす。
    * これらは単なるexeなのでデーモンとして起動するときは別途設定が必要。
    * walbcコマンドを使ってこれらのプロセスと通信し操作を行う。
    * 更にpython/walb/walb.pyを使うとより抽象度の高い操作ができる。
  * サービス構成
    * s0(storage) : port 10000
    * p0(proxy) :   port 10100
    * a0(archive) : port 10200
  * ディスク構成
    * 新しくパーティションを切る場所がない、うっかり失敗してもいいようにループバックデバイスを使ってやってみる。

* インストール
  * workディレクトリを作り
  ```
  mkdir work
  cd work
  git clone git@github.com:herumi/cybozulib.git
  git clone git@github.dev.cybozu.co.jp:starpos/walb.git
  git clone git@github.dev.cybozu.co.jp:starpos/walb-tools.git
  ```
  をする。
  * ドライバのbuildとインストール
    * kernelのバージョンに合わせてチェックアウトする。
      * kernel 3.13なら
      ```
      git co -b for-3.10 origin/for-3.10
      cd module
      make
      insmod walb-mod.ko
      ```
  * walb-toolsのbuild
    * clang++, gcc-4.8以降のC++11の機能を使う。
    * 各種ライブラリをインストールする。
    ```
    sudo apt-get install libaio-dev libsnappy-dev liblzma-dev zlib1g-dev
1    ```
    * walbとcybozulibにシンボリックリンクを張る。
    ```
    cd walb-tools
    ln -s ../walb .
    ln -s ../cybozulib .
    ```
    * buildする。
    ```
    make -j 8 DEBUG=0
    ```
    * binsrc/に各種exeができる。

* ディスクの準備
  * ループパックデバイス用に100MiBのファイルを作る。
  ```
  dd if=/dev/zero of=tutorial-disk bs=1k count=100k
  ```
  * /dev/loop0に割り当てる。
  ```
  sudo losetup /dev/loop0 tutorial-disk
  ```
  * 物理ボリュームを初期化する。
  ```
  sudo pvcreate /dev/loop0
  sudo pvs
  PV         VG   Fmt  Attr PSize   PFree
  /dev/loop0      lvm2 a--   100.00m  100.00m
  ```
  * LVをいくつか作る。
  ```
  sudo vgcreate tutorial /dev/loop0
  sudo lvcreate -n wdata -L 10m tutorial
  sudo lvcreate -n wlog -L 10m tutorial
  sudo lvcreate -n data -L 20m tutorial
  ```
    * /dev/tutorial/wdataと/dev/tutorial/wlogを合わせてwalbデバイスとして扱う。
  * /dev/tutorial/dataをproxy, archiveやシステムログ置き場にする。
  ```
  sudo mkfs.ext4 /dev/tutorial/data
  sudo mkdir -p /mnt/tutorial/data
  sudo mount /dev/tutorial/data /mnt/tutorial
  sudo mkdir -p /mnt/tutorial/data/{a0,p0,s0}
  ```

* tutorial-config.pyの作成
<work>/walb-tools/にtutorial-config.pyを作る。
```
#!/usr/bin/env python
import sys
sys.path.append('./python/walb/')
from walb import *

binDir = '/home/shigeo/Program/walb-tools/binsrc/'
wdevcPath = binDir + 'wdevc'
walbcPath = binDir + 'walbc'

def dataPath(s):
    return '/mnt/tutorial/data/%s/' % s

def logPath(s):
    return '/mnt/tutorial/data/%s.log' % s

s0 = Server('s0', 'localhost', 10000, K_STORAGE, binDir, dataPath('s0'), logPath('s0'))
p0 = Server('p0', 'vm4', 10100, K_PROXY, binDir, dataPath('p0'), logPath('p0'))
a0 = Server('a0', 'vm4', 10200, K_ARCHIVE, binDir, dataPath('a0'), logPath('a0'), 'tutorial')

sLayout = ServerLayout([s0], [p0], [a0])
walbc = Controller(walbcPath, sLayout, isDebug=False)

runCommand = walbc.get_run_remote_command(s0)
wdev0 = Device(0, '/dev/tutorial/wlog', '/dev/tutorial/wdata', wdevcPath, runCommand)

VOL = 'volm'
```
## ipythonでの使用例
* tutorial-conifg.pyの読み込み
tutorial-config.pyをwalb-toolsにおいてそのディレクトリで
```
sudo ipython
execfile('tutorial-config.py')
```
とする。
* サーバの起動
  * `sLayout.to_cmd_string()`でそのtutorial-config.pyに応じたwalb-{storage, proxy, archive}を起動するためのコマンドラインオプションが表示される。
  * これを使ってwalb-storage, walb-proxy, walb-archveをsudoで起動する。
* WalBデバイスの初期化
  1. /dev/tutorial/wlogの初期化
  `wdev0.format_ldev()`
  2. WalBデバイスの作成
  `wdev0.create()`
  * これでwdev0.path(通常/dev/walb/0)ができる。
* ボリューム(VOL)の初期化
  * `walbc.init_storage(s0, VOL, wdev0.path)`
* 状態の確認
  * `walbc.get_state_all(VOL)`でそれぞれのサーバがどういう状態かわかる。
  ```
  s0 localhost:10000 storage Slave
  p0 vm4:10100 proxy Started
  a0 vm4:10200 archive Archived
  ```
* /dev/walb/0にファイルシステムを作る。
  * ext4で初期化する。
  ```
  sudo mkfs.ext4 /dev/walb/0
  ```
  * mountする。
  ```
  sudo mkdir -p /mnt/tmp
  mount /dev/walb/0 /mnt/tmp
  ```
* full-backupをする。
  * storageをSyncReady状態にする。
  ```
  walbc.stop(s0, VOL)
  ```
  * フルバックアップを開始する。
  ```
  walbc.full_backup(s0, VOL)
  ```
  * このコマンドにより、storageの/dev/walb/0の全てのブロックをreadしてデータをarchiveに転送する。

* バックアップの復元をしてみる。
  * /mnt/tmpに適当にファイルを作る。 ***
  * ファイルを完全にディスクに書き終わらすためにumountする。
  ```
  sudo umount /dev/walb/0
  ```
  * snapshotをとる。
  ```
  walbc.snapshot(s0, VOL, [a0])
  ```
  * 表示された値がそのsnapshotに名付けられたgid。
  * restoreする。
  ```
  >walbc.restore(a0, VOL, <snapshotで返ってきたgid>)
  > 8
  ```
  * できたLVMのsnapshotは`get_restored_path`でわかる。
  ```
  > walbc.get_restored_path(a0, VOL, 8)
  > '/dev/tutorial/r_vol_8'
  ```
  * そのsnapshotのpathをmountする。
  ```
  sudo mount /dev/tutorial/r_vol_8 /mnt/tmp
  ```
  * /mnt/tmpの中に *** で書いたファイルがあることを確認する。
