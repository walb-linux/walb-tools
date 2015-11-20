# WalB-tools チュートリアル

このチュートリアルは WalB-tools(以下 tools) の簡単な使い方を説明します。
詳細は [README](README.md) を参照してください。

## WalB 概要
WalB 自体の概要は [WalB概要pptx](https://github.dev.cybozu.co.jp/herumi/walb-tools/raw/target/doc/walb-is-hard.pptx) を参照してください。


## 用語
詳しくは [用語一覧](word.md) - まだ作ってない - を参照。
ここでは最小限の言葉を記す。

* **WalB ブロックデバイス (wdev)**: WalB デバイスドライバがユーザに見せるブロックデバイス。
ユーザはこの上にファイルシステムを作ったり、パーティションを切ったりしてデータを置く。
* **wlog**: wdev に write したときに生成されるログ情報。
通常ユーザが直接見ることはない。
* **wdiff**: wlog をユーザが見える形式に変換したもの。
* **WalB ログデバイス (ldev)**: wlog が実際に書き込まれるデバイス。
* **WalB データデバイス (ddev)**: wdev に write したデータが実際に書き込まれるデバイス。
WalB は ldev と wdev をセットにして一つのデバイス wdev に見せている。
ユーザが wdev を通して見えるデータ領域。
* **snapshot をとる**: wdev のある瞬間における状態に名前をつけること。
* **gid**: snapshot をとったときにつけられる名前(一意な64bit整数)。
* **restore**: ある snapshot を LVM のボリュームに復元すること。
* **merge**: 複数の wdiff をまとめること。内部的には重複の除去、圧縮、ソートが行われる。
* **apply**: フルイメージを保持する LVM のボリュームに古い wdiff を適用して削除すること。
古い snapshot は復元できなくなるが使用領域を減らすことが出来る。
* **storage**: wdevのあるサーバ。ldev から wlog を取り出して proxyに 転送する。
* **proxy**: storage から受け取った wlog を wdiff に変換して一時保存する。
wdiff は archive に転送される。
* **archive**: proxy から受け取った wdiff を貯蔵する。フルイメージも保持。
* **full-backup**: wdev の全データを storage から archive に転送し、フルイメージとして保存すること。
* **hash-backup**: 全 wdev をチェックして必要な差分データを storage から archive に転送すること。
差分は wdiff として保存される。

## 最小構成

* PC 1台で walb-storage, walb-proxy, walb-archive を動かす。
  * これらは単なる exeなので デーモンとして起動するときは別途設定が必要。
  * walbc コマンドを使ってこれらのプロセスと通信し操作を行う。
  * 更に python/walblib/__init__.py を使うとより抽象度の高い操作ができる。
* サービス構成
  * s0(storage) : port 10000
  * p0(proxy) : port 10100
  * a0(archive) : port 10200
* ディスク構成
  * 新しくパーティションを切る場所がない、うっかり失敗してもいいようにループバックデバイスを使ってやってみる。
  * 全体像はこんな感じ ![レイアウト](layout.png)
    * wdev : /dev/walb/0
      * ddev : /dev/tutorial/mdata
      * ldev : /dev/tutorial/mlog
    * /mnt/tutorial/data/s0/ : storage サーバが管理するメータデータやログ情報を置く場所
    * /mnt/tutorial/data/p0/ : proxy サーバが管理するwdiffなどの情報を置く場所
    * /mnt/tutorial/data/a0/ : archive サーバが管理するwdiffなどの情報を置く場所
      * restore してできる LVM snapshot は /dev/tutorial/r_vol_??? の形になる。

## インストール

* work ディレクトリの作成

```
> mkdir work
> cd work
> git clone git@github.com:herumi/cybozulib.git
> git clone git@github.dev.cybozu.co.jp:starpos/walb.git
> git clone git@github.dev.cybozu.co.jp:starpos/walb-tools.git
```

* ドライバの build とインストール
  * kernel のバージョンに合わせてチェックアウトする。
    * kernel 3.13 なら
    ```
    git co -b for-3.10 origin/for-3.10
    cd module
    make
    insmod walb-mod.ko
    ```
* walb-tools の build
  * clang++, gcc-4.8 以降の C++11 の機能を使う。
  * 各種ライブラリをインストールする。
```
> sudo apt-get install libaio-dev libsnappy-dev liblzma-dev zlib1g-dev
```
* walb と cybozulib にシンボリックリンクを張る。

```
> cd walb-tools
> ln -s ../walb .
> ln -s ../cybozulib .
```

* build する。

```
> make -j 8 DEBUG=0
```

* binsrc/ に各種 exe ができる。

## ディスクの準備

* ループパックデバイス用に 100MiB のファイルを作る。

```
> dd if=/dev/zero of=tutorial-disk bs=1M count=100
```

* /dev/loop0 に割り当てる。

```
> sudo losetup /dev/loop0 tutorial-disk
```

* 物理ボリュームを初期化する。

```
> sudo pvcreate /dev/loop0
> sudo pvs
> PV         VG   Fmt  Attr PSize   PFree
> /dev/loop0      lvm2 a--   100.00m  100.00m
```

* LV をいくつか作る。

```
> sudo vgcreate tutorial /dev/loop0
> sudo lvcreate -n wdata -L 10m tutorial
> sudo lvcreate -n wlog -L 10m tutorial
> sudo lvcreate -n data -L 20m tutorial
```

* /dev/tutorial/wdata と /dev/tutorial/wlog を合わせて wdev として扱う。
* /dev/tutorial/data を proxy, archive やシステムログ置き場にする。

```
> sudo mkfs.ext4 /dev/tutorial/data
> sudo mkdir -p /mnt/tutorial/data
> sudo mount /dev/tutorial/data /mnt/tutorial
> sudo mkdir -p /mnt/tutorial/data/{a0,p0,s0}
```

## tutorial-config.py の作成

<work>/walb-tools/にtutorial-config.py を作る。

```
#!/usr/bin/env python
import sys
sys.path.append('./python')
from walblib import *

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
wdev = Device(99, '/dev/tutorial/wlog', '/dev/tutorial/wdata', wdevcPath, runCommand)

VOL = 'volm'
```

## ipython での使用例

### tutorial-conifg.py の読み込み

tutorial-config.py を walb-tools においてそのディレクトリで

```
> sudo ipython
> execfile('tutorial-config.py')
```

### サーバの起動

* `sLayout.to_cmd_string()` でその tutorial-config.py に応じた walb-{storage, proxy, archive} を起動するためのコマンドラインオプションが表示される。
* これを使って walb-storage, walb-proxy, walb-archve を sudo で起動する。

### WalB デバイスの初期化

1. /dev/tutorial/wlog の初期化
  `wdev.format_ldev()`
2. WalB デバイス の作成
  `wdev.create()`
* これで wdev.path (通常/dev/walb/0) ができる。

### ボリューム(VOL) の初期化

* `walbc.init_storage(s0, VOL, wdev.path)`

### 状態の確認

* `walbc.get_state_all(VOL)`でそれぞれのサーバがどういう状態かわかる。

```
s0 localhost:10000 storage Standby
p0 vm4:10100 proxy Started
a0 vm4:10200 archive Archived
```

### /dev/walb/0 にファイルシステムの作成

* ext4 で初期化する。

```
> sudo mkfs.ext4 /dev/walb/0
```

* mount する。

```
> sudo mkdir -p /mnt/tmp
> mount /dev/walb/0 /mnt/tmp
```

### full-backup

* storage を SyncReady 状態にする。
どの状態からどの状態にいけるのか大まかな説明は ![遷移図](state.png) を参照。

```
> walbc.stop(s0, VOL)
```

* フルバックアップを開始する。

```
> walbc.full_backup(s0, VOL)
```

* このコマンドにより、storage の wdev の全てのブロックを read してデータを archive に転送する。

### バックアップの復元

* /mnt/tmpに 適当にファイルを作る。 ***
* ファイルを完全にディスクに書き終わらすために umount する。

```
> sudo umount /dev/walb/0
```

* snapshot をとる。

```
> walbc.snapshot(s0, VOL, [a0])
> 8
```

* 表示された値がその snapshot に名付けられた gid。
* restore する。

```
> walbc.restore(a0, VOL, 8)
```

* できた LVM の snapshot は `get_restored_path` でわかる。

```
> walbc.get_restored_path(a0, VOL, 8)
> '/dev/tutorial/r_vol_8'
```

* その snapshot の path を mount する。

```
> sudo mount /dev/tutorial/r_vol_8 /mnt/tmp
```

* /mnt/tmp の中に ***で書いたファイルがあることを確認する。

* snapshot を削除する。
restore してできた LVM snapshot は `walbc.del_restored` で削除できる。
対象となる LVM snapshot が mount されていると削除できないのでまず umount が必要。
```
> walbc.del_restored(a0, VOL, 8) ; mount したまま実行
> 2014-11-12 07:03:56 ERROR Controller: error: c2aDelRestoredClient:child process has returned non-zero:1280
> cmd:/sbin/lvremove
> args:-f /dev/tutorial/r_vol_8
> stderr:  Logical volume tutorial/r_vol_8 contains a filesystem in use.
> umount /mnt/tmp
> walbc.del_restored(a0, VOL, 8) ; これで削除される
```

* restore した LVM snapshot一覧は `walbc.get_restored(a0, VOL)` で取得できる。

### merge
複数の wdiff は merge すると apply が速くなることがある。
また重複データが除去されるためデータサイズが小さくなることもある。
運用時には定期的に merge するとよい。
```
> walbc.print_diff_list(a0,VOL)
```
で wdiff を一覧できる。
```
 '|0|-->|1| -- 2014-11-11T07:12:14 4120',
 '|1|-->|2| -- 2014-11-11T07:12:42 17221',
 '|2|-->|3| M- 2014-11-11T07:14:30 8445',
 '|3|-->|4| M- 2014-11-11T07:14:33 8216',
 '|4|-->|5| M- 2014-11-11T07:14:38 8732',
 '|5|-->|6| M- 2014-11-11T07:15:00 8488',
 '|6|-->|7| M- 2014-11-11T07:15:18 8649',
 '|7|-->|8| M- 2014-11-11T07:15:43 4120',
 '|8|-->|9| -- 2014-11-11T07:15:52 8496',
 '|9|-->|10| M- 2014-11-11T07:16:01 8542',
 '|10|-->|11| M- 2014-11-11T07:16:11 9278',
 '|11|-->|12| M- 2014-11-11T07:16:17 8876',
```
`M` のマークがついた wdiff は merge できる。2から8までの wdiff を merge してみる。
```
> walbc.merge(a0,VOL,2,8)
> walbc.print_diff_list(a0,VOL)
 '|0|-->|1| -- 2014-11-11T07:12:14 4120',
 '|1|-->|2| -- 2014-11-11T07:12:42 17221',
 '|2|-->|8| M- 2014-11-11T07:15:43 5570',
 '|8|-->|9| -- 2014-11-11T07:15:52 8496',
 '|9|-->|10| M- 2014-11-11T07:16:01 8542',
 '|10|-->|11| M- 2014-11-11T07:16:11 9278',
 '|11|-->|12| M- 2014-11-11T07:16:17 8876',

```
8445, 8216, 8732, 8488, 8649, 4120byte の wdiff が merge されて 5570byte の wdiff になったことを確認できる。

### apply
古い snapshot を restore する必要がなくなると、apply することで古い snapshot に必要な wdiff が削除されてディスク容量を減らすことができる。
また restore にかかる時間も短縮できる。
```
> walbc.print_diff_list(a0,VOL)
['|0|-->|1| -- 2014-11-11T07:12:14 4120',
 '|1|-->|2| -- 2014-11-11T07:12:42 17221',
 '|2|-->|8| M- 2014-11-11T07:15:43 5570',
 '|8|-->|9| -- 2014-11-11T07:15:52 8496',
 '|9|-->|10| M- 2014-11-11T07:16:01 8542',
 '|10|-->|11| M- 2014-11-11T07:16:11 9278',
 '|11|-->|12| M- 2014-11-11T07:16:17 8876',
 ...
```
0～8までの wdiff を 0 に apply する。
```
> walbc.apply(a0, VOL, 8)
walbc.print_diff_list(a0,VOL)
Out[11]:
['|8|-->|9| -- 2014-11-11T07:15:52 8496',
 '|9|-->|10| M- 2014-11-11T07:16:01 8542',
 '|10|-->|11| M- 2014-11-11T07:16:11 9278',
 '|11|-->|12| M- 2014-11-11T07:16:17 8876',
 ...
```
apply されて 0～8 の diff が削除された。

### ハッシュバックアップ
なんらかの障害で proxy サーバのデータが飛んだときなどに
storage と archive の間で持っているデータの hash を比較して必要なものだけを転送する。
フルバックアップに比べて転送データ量が少なくてすむ。
ハッシュバックアップを試してみる。
storage を止める。
```
> walbc.get_state_all(VOL)
> s0 localhost:10000 storage Stopped
> p0 localhost:10100 proxy Started
> a0 localhost:10200 archive Archived
```
この状態でハッシュバックアップを行う。
```
walbc.hash_backup(s0, VOL)
```

### レプリケーション
* archive の非同期レプリケーションを行う。
最小構成にもう1個 archive サーバを加える。

* config.py の書き換え
```
a1 = Server('a1', 'localhost', 10201, K_ARCHIVE, binDir, dataPath('a1'), logPath('a1'), 'tutorial2')
```
を追加し、sLayout を
```
sLayout = ServerLayout([s0], [p0], [a0, a1])
```
に変更する。
* ボリュームの追加
新たに tutorial2-disk を作りそこに tutorial2 というボリュームグループを作る。
```
dd if=/dev/zero of=tutorial2-disk bs=1M count=50
sudo losetup /dev/loop1 tutorial2-disk
sudo pvcreate /dev/loop1
sudo vgcreate tutorial2 /dev/loop1
```
* サーバの再起動
ipython を起動し直して、`execfile('config.py')` して `sLayout.to_cmd_string()` の結果を使ってサーバを起動し直す。

* 状態の確認
```
> walbc.get_state_all(VOL)
> s0 localhost:10000 storage Target
> p0 localhost:10100 proxy Started
> a0 localhost:10200 archive Archived
> a1 localhost:10201 archive Clear
```
`a1` の追加直後は `Clear` 状態なので `SyncReady` 状態に持っていく。
```
> walbc._init(a1, VOL)

> walbc.get_state_all(VOL)
> s0 localhost:10000 storage Target
> p0 localhost:10100 proxy Started
> a0 localhost:10200 archive Archived
> a1 localhost:10201 archive SyncReady
```
レプリケーションを一度だけする。レプリケーションされたあと継続しない。
```
> walbc.replicate_once(a0, VOL, a1)
> 22
```
この gid(22) は環境によって変わる。
restore する。
```
> walbc.restore(a1, VOL, 22)
```
すると tutorial2 に r_vol_22 ができる。
`a0` 側も 22 を restore する。
```
> walbc.restore(a0, VOL, 22)
```
二つの sha1 が等しいことを確認する。
```
> sudo sha1sum /dev/tutorial/r_vol_22
> ff24c0b72da6491d6ec2288579257e7c423cedb3  /dev/tutorial/r_vol_22
> has:/home/shigeo/Program/walb-tools% sudo sha1sum /dev/tutorial2/r_vol_22
> ff24c0b72da6491d6ec2288579257e7c423cedb3  /dev/tutorial2/r_vol_22
```

* シンクロナイズモード
`replicate_once` は実行後はa0とa1は同期していない。
そのあとも常時動悸するようにするにはシンクロナイズモードに移行しなければならない。
```
> walbc.synchronize(a0, VOL, a1)
```
で同期モードになる。
今同期モードかそうでないかは `is_synchronizing` でわかる。
```
> walbc.synchronize(a0, VOL, a1)
> walbc.is_synchronizing(a1, VOL)
> True
> walbc.stop_synchronizing(a1, VOL)
> walbc.is_synchronizing(a1, VOL)
> False
```
最初からシンクロナイズモードでレプリケーションするには
`replicate_once` ではなく `replicate` を使えばよい。
