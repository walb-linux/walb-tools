# walb-tools apply/merge 再再考

## 3 つのモデル

- (1) Block モデル
  block 列としての snapshot，(addr,block) の集合としての diff，
  apply と merge 操作の定義．

- (2) 単純メタデータモデル
  walb-tools が想定する s,d の列を用いた定義と定理．
  定理を (1) を用いて証明．

- (3) 範囲メタデータモデル
  walb-tools が実装で用いる s,d,as,md 表現の定義と，
  s0 および d 列の生成ルールの定義．
  canApply()/canMerge()/apply()/applying()/merge() 操作の定義．
  表現および操作が (2) の条件および結果と一致することの証明．


## block モデル

### 定義: block

- 固定サイズ(一般に 512B or 4KiB)のデータ．
- equality 演算子 b0 == b1, b0 != b1
block はデータが時刻によって変動するときを表すときと，
ある瞬間のデータを表すときと両方の意味で用いられる．
時刻 t における block b のデータ明示したいときは b[t] と書く．

### 定義: block device

block device は n 個の block からなる block 列である．
block 列の各 block はアドレス a (a = 0, 1, ..., n-1) を用いて指定される．
アドレスの集合をA = {0, 1, ..., n-1} と書く(n は block device ごとに変わりうるが明記しない)．
block device b のアドレス a の block を b[a] と書く．
時刻 t におけるアドレス a の block は b[a][t] と書ける．

### 定義: clean/dirty snapshot

block device の各 block のある時刻の block data を集めたものを snapshot と呼ぶ．

全ての block のデータの時刻が同じであるとき，その snapshot を
clean snapshot と呼ぶ．
block device b の時刻 t0 における clean snapshot s は次の関係式を満たす．
```
s[a] = b[a][t0] for all a = 0, 1, ..., n-1.
```
`s` を `clean_snapshot(b, t0)` とも書く．

全ての block のデータの時刻が時刻 t0 から時刻 t1 の間のどれかの時刻であるとき，
その snapshot を dirty snapshot と呼ぶ．
block device b の時刻 t0 .. t1 における dirty snapshot s は次の関係式を満たす．
```
s[a] = b[a][t_a] for all a = 0, ..., n-1, t0 <= t_a <= t1.
```
`s` を `dirty_snapshot(b, t0, t1)` とも書く．

以下は自明である．
```
for all t,
dirty_snapshot(b, t, t) = clean_snapshot(b, t)
```

通常の block device を用いた場合，
オンライン(write IO が実行され得る状態)で全ブロックの
コピーを取ろうと思っても，コピー中に実行される write IO が存在する限り，
dirty snapshot しか得られない．


### 定義: diff

block device b のアドレスの集合 A の部分集合 A' と a in A' に対応する block
のある瞬間のデータ d[a] の組の集合 { (a, d[a]) | a in A' } を diff と呼ぶ．
A' を `supp d` とか `|d|` と書き，
アドレス a in |d| に対応する block を `d[a]` と書く．
`a not in |d|` の場合は，`d[a] = null` とする．
`|d| is empty` のとき，`d` を empty diff と呼ぶ．

### 定義: apply

snapshot `s` と diff `d` があったとき，以下の条件を満たす snapshot `s'` を，
`apply(s, d)` とか `s << d` と書く．また，`s` に `d` を apply (適用) する，などと言う．

```
for all a,
  s'[a] = d[a] if a in |d|
          s[a] otherwise
```

`(s << d0) << d1` を `s << d0 << d1` と略記する(演算子は左結合)．


### 定義: applying

snapshot `s` と diff `d` があったとき，以下の条件を満たす snapshot `s'` を，
`applying(s, d)` とか `s <: d` と書く．applying 状態もしくは apply 中状態とも呼ぶ．

```
for all a,
  s'[a] = s[a] or d[a] if a in |d|
          s[a]         if otherwise
```

a in |d| に対する `s'[a]` は s[a] か d[a] のどちらか不定(非決定的)であることに注意．

`(s <: d0) <: d1` を `s <: d0 <: d1` と略記する(左結合)．
また，`(s <: d0) << d1` を `s <: d0 << d1` と略記する(`<:` は `<<` より優先)．

`s'` は，`d` の一部のみ `s` に適用した状態だと考えることができる．


### 定義: merge

diff `d0` と `d1` があったとき，以下の条件を満たす diff `d2` を，
`merge(d0, d1)` とか `d0 ++ d1` と書く．`d0` に `d1` を merge すると言う．

```
for all a,
  d2[a] = d1[a] if a in |d1|
          d0[a] if a in |d0|-|d1|
          null  otherwise
```

`d0 ++ d1 = d1 ++ d0` であるとは限らない(交換法則を満たさない)．
また，`s <: (d0 ++ d1)` を `s <: d0 ++ d1` と略記する(`++` は `<:` より優先)．


### 定義: log diff

block device `b` に対してある期間 `[t0, t1]` に発生した write IO 集合を
`write_io_set(t0, t1)` と書き，次のように定義する．

`T` を `[t0, t1]` の部分集合とする．
任意の `t in T` に対して `A_t` をアドレス `[0, n)` の部分集合とする．

```
write_io_set(t0, t1) := { b[a][t] | t in T, a in A_t }
```

(同一時刻に同一アドレスに対する write IO は存在しない定義となっていることに注意．)


また，T_a を以下のように定義する．

```
T_a := { t | a in A_t }
```

`write_io_set(t0, t1)` から以下の条件で構成された diff `d` を，
`log_diff(write_io_set(t0, t1))` を書き，log diff と呼ぶ．
`log_diff(t0, t1)` と略すこともある．

```
for all a,
  d[a] = b[a][max(T_a)] if T_a is not empty
         null           otherwise
```

`T_a is not empty <==> a in |d|` であることに注意。

`t0` および `t1` 時点での snapshot `s0` と `s1` があったとき，
`d0 = log_diff(t0, t1)` を用いることで，
`s0 << d0 = s1` が成立する．


### 定義: compared diff

snapshot `s0` と `s1` があったとき，以下の条件満たす diff `d` を
compared_diff(s0, s1) と書き，compared diff と呼ぶ．

```
for all a,
  d[a] = s1[a] if s0[a] != s1[a]
         null  otherwise
```

`|d| = {a | s0[a] != s1[a]}` となることに注意．

`compared_diff(s0, s1) = compared_diff(s1, s0)` は `s0 = s1` のときのみ成り立つのは明らかで，
このとき empty diff となる．

(walb の実装においては，直接ブロックを比較するのではなく
 hash 値を用いることで compared diff 構成の際に発生する転送データを減らす．)


### 定理

まとめ

- Theorem b1: `(d0 ++ d1) ++ d2 = d0 ++ (d1 ++ d2)`
- Theorem b2: `s << d0 << d1 = s << d0 ++ d1`
- Theorem b3: `dirty_snapshot(x,y) << log_diff(z,w) = dirty_snapshot(w,max(w,y))`
- Theorem b4: `s << compared_diff(s, s') = s'`
- Theorem b5: `d ++ d = d`


#### Theorem b1: `(d0 ++ d1) ++ d2 = d0 ++ (d1 ++ d2)`

merge 操作は結合法則が成り立つ．

証明

```
d01 = d0 ++ d1 とおく．

d01[a] = d1[a] if a in |d1|
         d0[a] if a in |d0|-|d1|

(d01 ++ d2)[a] =  d2[a] if a in |d2|
                 d01[a] if a in |d01|-|d2|
               = d2[a] if a in |d2|
                 d1[a] if a in |d1|-|d2|
                 d0[a] if a in |d0|-|d1|-|d2|

d12 = d1 ++ d2 とおく．

d12[a] = d2[a] if a in |d2|
         d1[a] if a in |d1|-|d2|

(d0 ++ d12)[a] = d12[a] if a in |d12|
                  d0[a] if a in |d0|-|d12|
               = d2[a] if a in |d2|
                 d1[a] if a in |d1|-|d2|
                 d0[a] if a in |d0|-|d1|-|d2|

故に (d0 ++ d1) ++ d2 = d0 ++ (d1 ++ d2)
```


#### Theorem b2: `s << d0 << d1 = s << d0 ++ d1`

d0 と d1 を 順に apply する操作と，d0 と d1 を merge 後に apply する操作は結果が等しい．

証明

```
左辺を s0，右辺を s1 とおく．

s00 = s << d0 とおく．

s00[a] = d0[a] if a in |d0|
          s[a] if a not in |d0|

s0[a] =  d1[a] if a in |d1|
        s00[a] otherwise
      = d1[a]  if a in |d1|
        d0[a]  if a in |d0|-|d1|
         s[a]  otherwise

dx = d0 ++ d1 とおく．

dx[a] = d1[a] if a in |d1|
        d0[a] if a in |d0|-|d1|

s1[a] = dx[a] if a in |dx|
         s[a] otherwise
      = d1[a] if a in |d1|
        d0[a] if a in |d0|-|d1|
         s[a] otherwise

以上から s0 = s1．よって s << d0 << d1 = s << d0 ++ d1 が示された．
```

`s << d0 << d1 << ... < d_{n-1} = s << d0 ++ d1 ++ ... ++ d_{n-1}`
も成立する．


#### Theorem b3: `dirty_snapshot(x,y) << log_diff(z,w) = dirty_snapshot(w,max(w,y))`

ただし，z <= x and x < w を満たすものとする．

証明

```
s = dirty_snapshot(x, y)，
d = log_diff(z, w)
s' = s << d
とおく．

任意の a に対して，ある t_a in [x, y] があって
s[a] = b[a][t_a].

T_a is empty のとき，
write IO は [z, w] 区間で発生しなかったので，
全ての t in [z, w] について b[a][t] = b[a][z]．
これと，z <= x and x < w から，
w >= y のとき
  s[a] = b[a][w]
w < y のとき
  ある t in [w, y] にたいして s[a] = b[a][t].

T_a is not empty のとき，
log diff の定義より，

d[a] = b[a][max(T_a)] = b[a][w]

apply の定義により，

s'[a] = d[a] if a in |d|
        s[a] otherwise

w >= y のとき，
s'[a] = b[a][w] if a in |d|
        b[a][w] otherwise
      = b[a][w]

w < y のとき，
s'[a] = b[a][w]                  if a in |d|
        b[a][t] s.t. w <= t <= y otherwise
      = b[a][t] s.t. w <= t <= y

よって，
s' = dirty_snapshot(w, max(w, y))
故に，示された．
```

より単純なケースでは，以下が成立する．
```
t0 <= t1 <= t2 <= t3 のとき，
dirty_snapshot(t1,t2) << log_diff(t0,t3) = clean_snapshot(t3)
```


#### Theorem b4: `s << compared_diff(s, s') = s'`

証明

```
d = compared_diff(s, s') とおく．

compared_diff の定義により，任意の a について，
d[a] = s'[a] if s[a] != s'[a]
       null  otherwise

(s << d)[a] = s'[a] if s[a] != s'[a]
              s[a]  if s[a] == s'[a]
            = s'[a]

よって，示された．
```


#### Theorem b5: `d ++ d = d`

証明

```
merge の定義により，任意の a について，

(d ++ d)[a]
  = d[a] if a in |d|
    d[a] if a in |d|-|d|
    null otherwise
  = d[a] if a in |d|
    null otherwise
  = d[a]

よって示された．
```


## 単純メタデータモデル

### 定義

- ある block device b の適当な snapshot を選び，s0 とする．
- 以下のルールを用いて，`D = {d0, d1, ...}` `S = {s0, s1, ...}` を構成する．

```
ルール(1) か (2) のいずれかを選ぶ．

ルール(1)

  s_i に対して，適当な diff を選び，d_i とする．
  s_{i+1} := s_i << d_i を定義する．

ルール(2)

  s_i に対して，適当な snapshot を選び s_{i+1} とする．
  d_i := compared_diff(s_i, s_{i+1}) と定義する．
```

ルール(1) の場合は定義から，ルール(2) の場合は Theorem b4 より，
以下が導かれる．
```
for all i, s_i << d_i = s_{i+1}
```

#### merged diff `d_{i,j}`

diff 列の連続する部分列を merge したものを `d_{i,j}` と書く．

```
d_{i,j} := d_i ++ d_{i+1} ++ ... ++ d_{j-1}  if i < j
           empty diff                        if i = j
```


#### compared diff `d_{i:j}`

`s_i` と `s_j` から作られた compared diff を，`d_{i:j}` と書く．

```
d_{i:j} := compared_diff(s_i, s_j)  if i < j
           empty diff               if i = j
```


#### canMerge predicate

merged diff もしくは compared diff が 2 つ (`d0` と `d1`) あったとき，
以下の条件を満たす述語 canMerge を定義し，`canMerge(d0, d1)` とか `d0 +? d1` と書く．
ただし、`d0` と `d1` の diff の種類によって 4 通りに分けられる。

```
(1) d_{i,j} +? d_{k,l} := i <= k <= j < l
(2) d_{i:j} +? d_{k,l} := False
(3) d_{i,j} +? d_{k:l} := False
(4) d_{i:j} +? d_{k:l} := False
```

(1) は merge 後の diff が diff 列の連続部分列となる条件であることを意味する．
(2)(3)(4) は，compared diff は merge できないことを意味する．


#### applying snapshot

以下の条件を満たす applying snapshot `s_{i,j}` および `s_{i:j}` を定義する．
ただし、`i <= j` である。

```
for all a,
  s_{i,j}[a] = s_i[a], s_{i+1}[a], ..., s_j[a] のうちのいずれか(不定)

for all a,
  s_{i:j}[a] = s_i[a] か s_j[a] のいずれか(不定)
```

上記の定義から，以下が成り立つ．
```
s_{i,i} = s_{i:i} = s_i
```

また，
```
a not in |d_{i,j}| ==> s_i[a] = s_{i+1}[a] = ... = a_j[a]
a not in |d_{i:j}| ==> s_i[a] = s_j[a]
```
であることから，これらの条件を満たすアドレス `a` に関して，
`s_{i,j}[a]` や `s_{i:j}[a]` は一意に決まる．


#### canApply predicate

snapshot もしくは applying snapshot `s` と merged diff もしくは compared diff `d` があったとき，
以下のように述語 canApply を定義し，`canApply(s, d)` とか，`s <? d` と書く．
`s_(i,j)` は、`s_{i,j}` か `s_{i:j}` のどちらかを指すものとする。

```
(1) s_(i,j) <? d_{k,l} :=
      k <= i < l        if i == j
      k <= i and j <= l otherwise

(2) s_{i,j} <? d_{k:l} :=
      i == k < l        if i == j
      False             otherwise

(3) s_{i:j} <? d_{k:l} :=
      i == k < l        if i == j
      i == k and j == l otherwise
```


#### 重複のない diff 列

以下のルールで生成された diff 列を，重複のない diff 列か，`mcdl` と書く．

- diff 列が空の場合は，`d0, d_{0,i}, d_{0:i}` のいずれかを加える `(i > 0)`．
- diff 列が空でない場合は，その最後の diff `d_{j-1}, d_{i,j}, d_{i:j}` のいずれかに対して，
  `d_j, d_{j,k}, d_{j:k}` のいずれかを diff 列の最後に追加する `(j < k)`．

例: `d0, d1, d_{2:5}, d5, d_{6,8}, d_{8:10}, d10, d11`

このような構成法で生成された diff 列の集合を `MCDL` とする．
定義から d_{i,j} と d_{i:j} が同時に mcdl に含まれないことが導かれる．

ある mcdl in MCDL が与えられたとき，
mcdl[0] = mcdl とする．
mcdl[i+1] を，mcdl[i] の merge 可能な部分集合を merge したもので置き換えた集合とする．

ある `mcdl in MCDL` が与えられたとき，
`s0` から 0 回以上 `<< mcd` (ただし `mcd in mcdl`) 演算によって
到達できる snapshot の集合を supp mcdl とする．

また，supp2 mcdl = supp mcdl or { s <: d | s in supp mcdl, d in mcdl, s <? d}
とする．


### 補題＆定理

まとめ

- Lemma m1: `a in |d_{i,j}| ==> d_{i,j}[a] = s_j[a]`
- Lemma m2: `a in |d_{i:j}| ==> d_{i:j}[a] = s_j[a]`

- Lemma my1: `d = d_(i,j) のとき a in |d| ==> d[a] = s_j[a]`

- Lemma m3: `s_i << d_{j,i} = s_i`
- Lemma m4: `s_{i,j} << d_{k,i} = s_{i,j}`
- Lemma m5: `a not in |d_{i,j}| ==> a not in |d_{i:j}|`
- Lemma m6: `a not in |d_{i,j}| ==> a not in |d_{i',j'}| (i <= i', j' <= j)`
- Lemma m7: `s = s_{i:j} ==> s = s_{i,j}`

- Theorem mx1: `d +? d' ==> d ++ d' = d_{i,l} s.t. d = d_{i,j}, d' = d_{k,l}`
- Theorem mx2: `d +? d' ==> d ++ d' = d_{i,l} s.t. d = d_(i,j), d' = d_(k,l)`

- Theorem  m8: `s_i <? d_{k,l} ==> s_i << d_{k,l} = s_l`
- Theorem mx8a: `d = d_{k~l} のとき s_i <? d ==> s_i << d = s_l`
- Theorem  m9: `s_i <? d_{k:l} ==> s_i << d_{k:l} = s_l`
- Theorem m10: `s_i <? d_{k,l} ==> s_i <: d_{k,l} = s_{i:l}`
- Theorem m11: `s_i <? d_{k:l} ==> s_i <: d_{k:l} = s_{i:l}`
- Theorem m12: `s_{i,j} <? d_{k,l} ==> s_{i,j} << d_{k,l} = s_l`
- Theorem m13: `s_{i:j} <? d_{k:l} ==> s_{i:j} << d_{k:l} = s_l`
- Theorem m14: `s_{i,j} <? d_{k,l} ==> s_{i,j} <: d_{k,l} = s_{i,l}`
- Theorem m15: `s_{i:j} <? d_{k:l} ==> s_{i:j} <: d_{k:l} = s_{i:l}`

- Theorem m16: `for all mcdl in MCDL, for all s in supp mcdl[i], exists d in mcdl[j] s.t. s <? d, i <= j`
- Theorem m17: `for all mcdl in MCDL, for all as in supp2 mcdl[i], exists d in mcdl[j] s.t. s <? d, i <= j`


#### Lemma m1: `a in |d_{i,j}| ==> d_{i,j}[a] = s_j[a]`

証明

```
merged diff の定義と定理 b2 により，

s_i << d_{i,j}
  = s_i << d_i ++ d_{i+1} ++ ... ++ d_{j-1}
  = s_i << d_i << d_{i+1} << ... << d_{j-1}
  = s_j

apply の定義により，

s_j[a] = d_{i,j}[a] if a in |d_{i,j}|

よって，示された．
```

#### Lemma m2: `a in |d_{i:j}| ==> d_{i:j}[a] = s_j[a]`

証明

```
compared diff の定義により，

s_i << d_{i:j} = s_j

apply の定義により，

s_j[a] = d_{i:j}[a] if a in |d_{i:j}|

よって，示された．
```

#### Lemma my1: `d = d_(i,j) のとき a in |d| ==> d[a] = s_j[a]`

証明

```
d = d_{i,j} のとき，
対応する時刻 T_i と T_j がある．
T_i < T_j．
d = log_diff(T_i, T_j)
a in |d| のとき，
定義より，t in [max(T_a), T_j] において b[a][t] は変化しない．
すなわち，b[a][T_j] = s_j[a] = d[a] if a in |d|．


d = d_{i:j} のとき，
定義から，
d[a] = s_j[a] if s_i[a] != s_j[a] <==> a in |d|．

よって，示された．
```


#### Lemma m3: `s_i << d_{j,i} = s_i`

証明

```
定義より，

d_{j,i}[a] = s_i[a] if a in |d_{j,i}|

sx = s_i << d_{j,i} とすると，apply の定義により，

sx[a] = s_i[a] if a in |d_{j,i}|
        s_i[a] otherwise

すなわち sx = s_i
よって，示された．
```


#### Lemma m4: `s_{i,j} << d_{k,i} = s_{i,j}`

証明

```
s_{i,j} の定義から，

s_{i,j}[a] = s_i[a], s_{i+1}, ..., s_j[a] のいずれか

補題から，

d_{k,i}[a] = s_i[a] if a in |d_{k,i}|

apply の定義から，

(s_{i,j} << d_{k,i})[a]
  = s_i[a]                                 if a in |d_{k,i}|
    s_i[a], s_{i+1}, ..., s_j[a] のいずれか otherwise
  = s_{i,j}

よって示された．
```


#### Lemma m5: `a not in |d_{i,j}| ==> a not in |d_{i:j}|`

証明

```
左辺の条件から，s_i[a] = s_j[a] が成り立つ．
このとき，d_{i:j}[a] は，compared diff の定義により null となる．
よって，示された．
```

#### Lemma m6: `a not in |d_{i,j}| ==> a not in |d_{i',j'}| (i <= i', j' <= j)`

証明

```
merge の定義により，
a not in |d_{i,j}| <==> a not in |d_x| for all i <= x <= j
==> a not in |d_x| for all i' <= x <= j'
<==> a not in |d_{i',j'}|

よって示された．
```

#### Lemma m7: `s = s_{i:j} ==> s = s_{i,j}`

証明

```
for all a,
  s_{i:j}[a] = s_i[a], s_j[a] のいずれか
  s_{i,j}[a] = s_i[a], s_{i+1}[a], ..., s_j[a] のいずれか

故に，s = s_{i:j} ==> s = s_{i,j}
```


#### Theorem mx1: `d +? d' ==> d ++ d' = d_{i,l} s.t. d = d_{i,j}, d' = d_{k,l}`

証明

```
canMerge の定義により，

i <= k <= j < l

d_{i,j} ++ d_{k,l}
  = d_{i,k} ++ d_{k,j} ++ d_{k,j} ++ d_{j,l}
(定理 b5 により)
  = d_{i,k} ++ d_{k,j} ++ d_{j,l}
  = d_{i,l}

よって，示された．
```


#### Theorem mx2: `d +? d' ==> d ++ d' = d_{i,l} s.t. d = d_(i,j), d' = d_(k,l)`

証明

```
演算子 (+?) の定義により，
d +? d' が True になるのは，
d = d_{i,l}, d' = d_{k,l} であるときなのは明らか．

よって，Theorem mx1 より，示された．
```


#### Theorem  m8: `s_i <? d_{k,l} ==> s_i << d_{k,l} = s_l`

証明

```
s_i <? d_{k,l} = k <= i < l

s_i << d_{k,l} = s_i << d_{k,i} ++ d_{i,l}
               = s_i << d_{k,i} << d_{i,l}
Lemma m3 より
               = s_i << d_{i,l}
               = s_l
```

#### Theorem mx8a: `d = d_{k~l} のとき s_i <? d ==> s_i << d = s_l`

これは Theorem m8 の拡張．

証明

```
d = d_{k,l} のとき，Theorem m8 より示された．

d = d_(k:l} のとき

s_i <? d
  = s_i <? d_{k:l}
  <==> i = k

s_i << d
  = s_i << d_{k:l}
  = s_i << d_{i:l}
  = s_l

よって示された．
```


#### Theorem  m9: `s_i <? d_{j:k} ==> s_i << d_{j:k} = s_k`


証明

```
canApply の定義より，i = j．

j = k のとき

d_{j:k} = d_{i:i} = empty diff
故に s_i << d_{i:i} = s_i = s_k

j < k のとき

compared diff の定義より，

s_i << d_{j:k} = s_i << d_{i:k} = s_k

よって示された．
```


#### Theorem m10: `s_i <? d_{j,k} ==> s_i <: d_{j,k} = s_{i:k}`

証明

```
canApply の定義により，j <= i <= k．

sx = s_i <: d_{j,k} とおく．

applying の定義により，任意の a について，

sx[a] = s_i[a] or d_{j,k} (= s_k[a]) if a in |d_{j,k}|
        s_i[a]                        if a not in |d_{j,k}|

Lemma m6 より，
a not in |d_{j,k}| ==> a not in |d_{i,k}|

Lemma m5 より，
a not in |d_{i,k}| ==> a not in |d_{i:k}|

sx[a] = s_i[a]           if a not in |d_{j,k}|
        s_i[a] or s_k[a] otherwise
      = s_i[a]           if a not in |d_{i,k}|
        s_i[a] or s_k[a] otherwise
      = s_i[a]           if a not in |d_{i:k}|
        s_i[a] or s_k[a] otherwise
      = s_{i:k}[a]

すなわち，sx = s_{i:k}．
よって示された．
```


#### Theorem m11: `s_i <? d_{j:k} ==> s_i <: d_{j:k} = s_{i:k}`

証明

```
canApply の定義により，i = j <= k．

sx = s_i <: d_{i:k} とおく．

applying の定義により，任意の a について，

sx[a] = s_i[a] or d_{i:k}[a] (= s_k[a]) if a in |d_{i:k}|
        s_i[a]                           otherwise
      = s_i[a] or s_k[a]
      = s_{i:k}[a]

すなわち，sx = s_{i:k}．
よって示された．
```


#### Theorem m12: `s_{i,j} <? d_{k,l} ==> s_{i,j} << d_{k,l} = s_l`

証明

```
canApply の定義により，i >= k, j <= l

sx = s_{i,j} << d_{k,l} とおく．

sx = s_{i,j} << d_{k,l}
   = s_{i,j} << d_{k,i} ++ d_{i,l}
   = s_{i,j} << d_{k,i} << d_{i,l}
Lemma m3 より
   = s_{i,j} << d_{i,l}

su_i_j[a] を s_i[a], s_{i+1}[a], ..., s_j[a] のいずれかを表すものとする．

s_{i,j} の定義より，
a not in |d_{i,j}| ==> s_i[a] = s_{i+1}[a] = ... = s_j[a]．
であることに注意すると，任意の a について，

s_{i,j}[a] = su_i_j[a] if a in |d_{i,j}|
             s_i[a]    if a not in |d_{i,j}|

Lemma m6 の対偶より，

a in |d_{i,j}| ==> a in |d_{i,l}|

すなわち，|d_{i,l}| は |d_{i,j}| を含む．

apply の定義により，

sx[a]
  = d_{i,l}[a] if a in |d_{i,l}|
    s_{i,j}[a] if a not in |d_{i,l}|
  = s_l[a]     if a in |d_{i,l}|
    su_i_j[a]  if a in |d_{i,j}|-|d_{i,l}|
    s_i[a]     if a not in (|d_{i,l}| or |d_{i,j}|)

|d_{i,j}|-|d_{i,l}| is empty であり，
a not in d_{i,l} ==> s_i[a] = s_l[a] となる．よって，

sx[a] = s_l[a]     if a in |d_{i,l}|
        s_l[a]     if a not in |d_{i,l}|
      = s_l[a]

故に，示された．
```


#### Theorem m13: `s_{i:j} <? d_{k:l} ==> s_{i:j} << d_{k:l} = s_l`

証明

```
canApply の定義により，i = k, j = l.

sx = s_{i:j} << d_{i:j} とおく．

s_{i:j} の定義より，

s_{i:j}[a] = s_i[a] か s_j[a] のいずれか

apply の定義より，

sx[a] = d_{i:j}[a] if a in |d_{i:j}|
        s_{i:j}[a] if a not in |d_{i:j}|

a not in |d_{i:j}| ==> s_{i:j}[a] = s_i[a] = s_j[a] であり，
a in |d_{i:j}| ==> d_{i:j}[a] = s_j[a] であることから，

      = s_j[a] if a in |d_{i:j}|
        s_j[a] otherwise
      = s_j[a]

よって，sx = s_j = s_l．
故に，示された．
```


#### Theorem m14: `s_{i,j} <? d_{k,l} ==> s_{i,j} <: d_{k,l} = s_{i,l}`

証明

```
canApply の定義により，i >= k, j <= l．

sx = s_{i,j} <: d_{k,l} とおく．

また，su_i_j[a] を s_i[a], s_{i+1}[a], ..., s_j[a] のいずれかを表すものとする．

applying snapshot の定義から，

s_{i,j}[a] = su_i_j[a]

applying の定義より，

sx[a]
  = su_i_j[a] か s_l[a] のいずれか if a in |d_{k,l}|
    s_{i,j}[a]                   otherwise
  = su_i_l[a] if a in |d_{k,l}|
    su_i_j[a] otherwise
  = su_i_l[a]
  = s_{i,l}

故に，sx = s_{i,l}．
よって，示された．
```


#### Theorem m15: `s_{i:j} <? d_{k:l} ==> s_{i:j} <: d_{k:l} = s_{i:l}`

証明

```
canApply の定義より，i = k, j = l．

sx = s_{i:j} <: d_{i:j} とおく．

s_{i:j} の定義より，

s_{i:j}[a] = s_i[a] or s_j[a]

applying の定義より，

sx[a] = s_{i:j}[a] or d_{i:j}[a] if a in |d_{i:j}|
        s_{i:j}[a]               otherwise
      = s_{i:j}[a]

故に，sx = s_{i:j} = s_{i:l}．
よって，示された．
```


#### Lemma m16a: `supp mcdl = {s0} or {s_j | d_(i,j) in mcdl}`

証明

```
定義から s0 は明らかに supp mcdl に含まれる．

構築中の mcdl の最後の diff を s_j とする(diff 列が空の場合は j = 0)．
s_j に対して，j < k なる d_j もしくは d_{j,k} もしくは，d_{j:k} のいずれかが追加される．
ただし，d_j = d_{j,j+1} であるので，d_{j,k} もしくは d_{j:k} のいずれかとなる．
このとき，s_j << d_{j,k} = s_k, s_j << d_{j:k} = s_k となり，
いずれの場合も s_k となり，これは supp mcdl に含まれる．

すなわち
d_{j,k} もしくは d_{j:k} が mcdl に含まれている
<==> s_k が supp mcdl に含まれる．

よって，示された．
```


#### Theorem m16: `for all mcdl in MCDL, for all s in supp mcdl[i], exists d in mcdl[j] s.t. s <? d, i <= j`

apply 可能 diff の存在．


証明

```
i = j のとき，成立するのは明らか．

mcdl の定義より，
d_{i,j} に対して，d_{j,k}, d_{k,l}, ... d_{y,z} が mcdl に含まれる場合に限り，
merge 可能である．
mcdl[i] において，そのような diff 部分集合を merge したもので置き換えたとき，
d_{i,j} ++ d_{k,l} ++ ... ++ d_{y,z} = d_{i,z} であるので，
mcdl[i+1] = mcdl[i] - {d_{i,j}, d_{k,l}, ..., d_{y,z}} or {d_{i,z}} となる．
このとき，
Lemma m16a により，
supp mcdl[i+1] = supp mcdl[i] - {s_j, s_l, ..., s_z} or {s_z} となる．
{s_j, s_l, ..., s_z} - {s_z} に含まれる任意の s に対して，
s <? d_{i,z} が成立し，s << d_{i,z} = s_z となる．

すなわち，for all s in supp mcdl[i], exists d in mcdl[i+1] s.t. s <? d
が成立する．

i <= j まで成立していると仮定する．

mcdl[j] に含まれる merge 可能な diff の部分集合に，mcdl[j-1] で置き換えた s_z が含まれない場合

j = i + 1 のときと同様に，成立する．

mcdl[j] に含まれる merge 可能な diff の部分集合に，mcdl[j-1] で置き換えた d_{i,z} が含まれる場合

d_{i,z} を含む merge 可能な部分集合を merge したものは，
p <= i, q <= z である p, q を用いて d_{p,q} となる．

canApply の定義により，d_{i,z} が適用可能な任意の snapshot に，d_{p,q} は適用可能．
よって，i <= j + 1 においても成立する．

以上から，数学的帰納法により，
for all mcdl in MCDL, for all s in supp mcdl[i], exists d in mcdl[j] s.t. s <? d, i <= j
示された．
```


#### Theorem m17: `for all mcdl in MCDL, for all as in supp2 mcdl[i], exists d in mcdl[j] s.t. as <? d, i <= j`

証明

```
d = d_{m:n} の場合

d は他のどの diff とも merge 不可能なので，mcdl に存在するならば，
必ず mcdl[i] for all i に含まれる．
また，d_{m:n} が mcdl に含まれる場合，m < p < n なる s_p は supp mcdl に含まれない．

d = d_{m:n} が apply 可能な任意の as = s_(k,l) について，
Theorem m13 と m15 により，
as <? d_{m:n} ==> (as <: d_{m:n}) <? d_{m:n}
が成立．


d = d_{m,n} の場合

Theorem m16 と同様に，i <= j であるとき，
d_{m,n} in mcdl[i] に対して，p <= m, n <= q であるような
d_{p,q} in mcdl[j] が必ず存在する．

d = d_{m,n} が apply 可能な任意の as = s_(k,l) の条件は，

s_(k,l) <? d_{m,n} =
  m <= k < n        if k == l
  m <= k and l <= n otherwise

すなわち，
p <= k < q        if k == l
p <= k and l <= q otherwise
は成立するため，
Theorem m12 と m14 により，
as <? d_{m,n} ==> (as <: d_{m,n}) <? d_{p,q}
が成立．

よって，示された．
```


### log diff と compared diff 違いが apply に及ぼす影響の考察

compared diff でも d_{i:i+1} は d_i と同等に扱っても差し支えないが，
d_{i:j} (i + 1 < j) の場合は，merge してしまうと apply 対象 snapshot によっては
取り零しが出てしまい，apply 結果が不整合になってしまう場合がある．
また，applying においても支障が出ると考えられる．
どの部分が compared diff 相当かの情報を付加すれば merge や apply の条件を緩和することが
可能だと思われるが，必要な情報が増え，実装が複雑になる割にはメリットは少ないと判断した．
merge や apply の条件を絞ることにより，
d_{i,j} と d_{i:j} とを区別するフラグを用意するだけで済む．
現実には d_{i:j} (i + 1 < j) は hash-repl でしか生成し得ない．
hash-repl の発生頻度は高くなく，生成された compared diff は多くの場合 base image に対して
すぐに apply してしまう運用が予想されるため，compared diff に対しては
merge を制限する制約を入れても問題にはならないことが推察される．
また，現実には d_{i:j} の i + 1 == j の判別のために追加の情報を保持するのは割に合わないため，
hash-repl 発生時は必ずフラグを立てることでモデルの破綻を防ぐ．
以上のように merge 条件を絞ってあるので，
applying においては，元の snapshot 情報と，apply 結果となる snapshot 情報を保持しておけば，
canApply と同等の操作で apply 可否を決定できる．


たとえば，
`dx = d_{0,2} ++ d_{2:4} ++ d_{4,6}`
として，dx を適用できる snapshot `sx` について考えてみると，
`s0, s1, s2, s4, s5, (s6)` に適用できることが分かる．
すなわち `dx` は `s3` には適用できない．
ちなみに，`d_{0,6}` は `s0, s1, s2, s3, s4, s5, (s6)` に適用でき，
`d_{0:6}` は `s0, (s6)` に適用できる．

よって，`dx` を `d_{0:6}` として扱うことは可能であるが，
そうすると，例えば，
`dx` を作る前に `s2` 状態の block device (bdev) に対して `d_{2:4}` を適用しようとし，
適用完了前にサーバダウンしたものとする．すると，bdev は `s_{2,4}` 状態になっている．
このとき，`d_{2:4}` は再び適用可能であるが，
`dx` を作成後に `d_{0,2}, d_{2:4}, d_{4,6}` を削除してしまった場合，
実際には `dx` を適用しても問題ないが，`d_{0:6}` という扱いであると，
canApply が False となるため，適用可能な diff が存在しないことになってしまう．

`dx` が適用しても問題ないことを判断するためには，
どの部分が compared 相当でどの部分がそうでないかの情報を保持する必要があるが，
それは実装が複雑になってしまうので，merge の制約を広げることで対応することにした．．



## 範囲メタデータモデル

ここでは，シーケンスではなく範囲を用いた snapshot と diff 表現，
および canApply, applying, apply, canMerge, merge を定義し，
それが，単純メタデータモデルと等価であることを示す．

範囲メタデータモデルが必要な理由:
- clean snapshot と dirty snapshot の判別
- dirty snapshot に必要な分の log diff を apply すると
  clean snapshot になるロジックの表現
- 一度に生成する log diff のサイズを制限したいため，
  (次の clean snapshot id を +1 ではなく，+x として生成したい)

### 定義

#### snapshot

meta snapshot `s` を `|B,E|` と書く．ただし，`B`, `E` は 0 以上の整数で，`B <= E` とする．
また，`B = E` のとき，`|B|` と略記する．

`B` および `E` は時刻に相当する値であり，
`B < E` であれば `B` より `E` は後の時刻あることを意味し，
`B = E` であれば，同時刻であることを意味する．

いくつかの表記を定義する．
```
s.B := B
s.E := E
s.is_dirty := (B < E)
s.is_clean := (B == E)
```

`s.is_clean` が `True` のとき， clean snapshot と呼び，
`False` のとき，dirty snapshot と呼ぶ．


#### diff

meta diff `d` を `s0-->s1` とか `|B,E|-->|B',E'|` と書く．
ただし，`s0` や `s1` は meta snapshot であり，`s0 = |B,E|`，`s1 = |B',E'|` とする．
また，`B < B'` とし，これを progress rule と呼ぶ．

いくつかの表記を定義する．
```
d.B := s0
d.E := s1

d.may_skip := True または False
d.is_dirty := (d.B.is_clean and d.E.is_clean)
d.is_clean := not d.is_dirty
```

`d.is_clean` が `True` のとき，clean diff と呼び，
`False` のとき，dirty diff と呼ぶ．
`may_skip` については後述．


#### canMergeR

canMergeR (演算子 `++?`) を次のように定義する．

```
d0 ++? d1 := d0.B.B <= d1.B.B <= d0.E.B < d1.E.B
             and not d0.may_skip
             and not d1.may_skip
```

それぞれの条件は以下を意味する．
- `d0.B.B <= d1.B.B` d0 の方がより古い snapshot に適用できること
- `d1.B.B <= d0.E.B` d0 と d1 は重複もしくは連続していること
- `d0.B.B < d1.B.B` d1 の方がより新しい snapshot を生成すること


#### mergeR

mergeR (演算子 `+++`) を次のように定義する．

```
d0 +++ d1 := d0.B-->|d1.E.B,max(d1.E.B,d0.E.E)| if d1.is_clean
             d0.B-->d1.E                        otherwise
```


#### applying snapshot

applying snapshot を `<s0, s1>` と書く．
ただし，`s0` と `s1` は snapshot で，`s0.B <= s1.B` とする．
`<s, s>` は `<s>` と略記する．

いくつかの表記を定義する．
```
as.is_applying := (as.s0 == as.s1)
as.B := s0
as.E := s1
```


#### canApplyR

述語 canApplyR (演算子 `<<?`) を snapshot s，applying snapshot as，
および diff d に対して次のように定義する．

```
s <<? d := d.B.B <= s.B < d.E.B
as <<? d := d.B.B <= as.B.B < d.E.B and as.E.B <= d.E.B
```

as == s のとき，3 番目の条件は、2 番目の条件に含まれるため、
上記 2 つの述語は等しい．


#### applyR

applyR (演算子 `<<<`) を snapshot s，applying snapshot as，
および diff d に対して次のように定義する．

```
s <<< d := |d.E.B, max(d.E.B, s.E)| if d.is_clean
           d.E                      otherwise

as <<< d := as.B <<< d
```

`as == s` のとき，上記 2 つの述語は等しい．


#### applyingR

applyingR (演算子 `<<:`) snapshot s，applying snapshot as，
および diff d に対して次のように定義する．

```
s <<: d := <s, s <<< d>
as <<: d := <as.B, as.B <<< d>
```


#### snapshot/diff の生成ルール

meta snapshot `ms0 = |0, x|` とする．ただし，`x` は自然数とする．
また，snapshot `s0 := dirty_snapshot(0, x)` とする．

`ms_i` (`s_i`) が与えられているとき，
`md_i` (`d_i`) および `ms_{i+1}` (`s_{i+1}`) を
以下のルールに従って構成することにより，
meta snapshot 列 `MS := {ms0, ms1, ...}`，
snapshot 列 `S := {s0, s1, ...}`，
meta diff 列 `MD := {md0, md1, ...}`，
diff 列 `D := {d0, d1, ...}` を構成する．

```
ルール(1) か (2) のいずれかを選ぶ．

ルール(1)

  ms_i に対して，B = ms_i.B と置き，B' を B < B' として取る．

  md_i := |B|-->|B'| と
  ms_{i+1} := ms_i <<< md_i を定義する．

  d_i := log_diff(B, B')，
  s_{i+1} := s_i << d_i も定義する．

ルール(2)

  ms_i に対して，B', E' を ms_i.E < B' < E' として取る．

  ms_{i+1} := |B',E'|，
  md_i := ms_i-->ms_{i+1} と定義する．

  s_{i+1} := dirty_snapshot(B', E')，
  d_i := compared_diff(s_i, s_{i+1}) と定義する．
```

注意:
  ms_i.B < B' < E' ではなく ms_i.E < B' < E' にしないと
  Theorem rx5 が言えない．

ルール(1)は wlog から diff および snapshot を生成するときで，
ルール(2)は hash-bkp のときに dirty snapshot から diff を生成するので，
生成順が違うことに注意．

`ms_{i+1}` は `<<<` の定義により、以下のようになる。
```
ルール(1) のとき
  md_i is clean
  md_i = |B|-->|B'|
  ms_{i+1} = |B', max(B', ms_i.E)|
  B = ms_i.B = md_i.B.B
  B' = md_i.E.B = m_{i+1}.B

ルール(2) のとき
  md_i is dirty
  md_i = ms_i-->|B',E'|
  ms_{i+1} = |B',E'|
  B' = d_i.E.B
  E' = d_i.E.E
```

上記から任意の `i` について `ms_i.B = md_i.B.B < md_i.E.B = ms_{i+1}.B` が成り立つ．
また，`i < j ==> ms_i.E <= ms_j.E` が成り立つ．

md_i.may_skip = False とする．

ここで `S` と `D` の構成法は，単純メタデータモデルでの構成法と一致する．

`i < j` であるような `s_i, s_j in S` において，
`md_{i:j} := ms_i-->ms_j` と定義する．
また，md_{i:j}.may_skip = True とする．


以下のように変換 M2B を定義する．
```
M2B(ms_i) := dirty_snapshot(ms_i.B, ms_i.E)

M2B(md_i) := log_diff(md_i.B.B, md_i.E.B) if md_i is clean
             compared_diff(s_i, s_{i+1})  otherwise

M2B(md_{i,j}) := M2B(md_i) ++ M2B(md_{i+1}) ++ ... ++ M2B(md_{j-1})
M2B(md_{i:j}) := compared_diff(M2B(ms_i), M2B(ms_j))
```


### 補題＆定理

後述．

-----

## インクリメンタルにモデルを拡張する．

- level1: apply only
- level2: apply, merge
- level3: apply, merge, compared_diff
- level4: apply, merge, compared_diff, applying


### Level1: apply only


```
単純メタデータモデル(level1: apply only)

s_i <? d_j := i == j

s_i << d_i = s_{i+1}


範囲メタデータモデル(level1: apply only)

ms <<? md := md.B.B <= ms.B < md.E.B
ms <<< md := |md.E.B, max(md.E.B, ms.E)| if md is clean
             md.E                        otherwise

Theorem ry1: for all i, j ms_i <<? md_j <==> i == j (<==> s_i <? d_j)
Theorem ry2: for all i M2B(md_i) = d_i, M2B(ms_i) = s_i
```


#### Theorem ry1: `for all i, j ms_i <<? md_j <==> i == j (<==> s_i <? d_j)`

証明

```
B_i = s_i.B とおく．

progress rule より B_i < B_j <==> i < j

pred = ms_i <<? md_j とおく．

i == j のとき

pred = B_i <= B_i and B_i < B_{i+1}
     = True

i < j のとき

pred = B_j <= B_i and B_i < B_{j+1}
     = False and True
     = False

i > j のとき

pred = B_j <= B_i and B_i < B_{j+1}
     = True and False
     = False


以上から，全ての i,j について ms_i << ? md_j <==> i == j が示された．
```


#### Theorem ry2: `for all i M2B(md_i) = d_i, M2B(ms_i) = s_i`

証明

```
M2B(ms0)
  = dirty_snapshot(ms0.B, ms0.E)
  = dirty_snapshot(0, x)
  = s0

0 から i まで M2B(ms_i) = s_i が，
0 から i-1 まで M2B(md_i) = d_i が成り立っていると仮定する．
このとき，
M2B(ms_{i+1}) = s_{i+1}
M2B(md_i) = d_i
を示す．

ルール(1) のとき
  B' = d_i.E.B = ms_{i+1}.B とする．
  ms_{i+1} = |B', max(B', ms_i.E)|
  M2B(ms_{i+1})
    = dirty_snapshot(ms_{i+1}.B, ms_{i+1}.E)
  s_{i+1}
    = s_i << d_i
    = M2B(md_i) << d_i
    = dirty_snapshot(ms_i.B, ms_i.E) << log_diff(ms_i.B, B')
ms_i.B <= ms_i.B, ms_i.B < B' を満たすので，Theorem b3 より
    = dirty_snapshot(B', max(B', ms_i.E))
    = dirty_snapshot(ms_{i+1}.B, ms_{i+1}.E)
  よって，M2B(ms_{i+1}) = s_{i+1}

  M2B(md_i)
    = log_diff(md_i.B.B, md_i.E.B)
    = d_i

ルール(2) のとき
  M2B(ms_{i+1})
    = dirty_snapshot(ms_{i+1}.B, ms_{i+1}.E)
    = s_{i+1}

  M2B(md_i)
    = compared_diff(s_i, s_{i+1})
    = d_i

よって、示された。
```



### Level2: apply, merge
```

単純メタデータモデル(level2: apply, merge)

d_{i,j} := d_i ++ d_{i+1} ++ ... ++ d_{j-1}
d_{i,j} +? d_{k,l} := i <= k <= j < l
s_i <? d_{k,l} := k <= i < l

Thorem m8: s_i <? d_{k,l} ==> s_i << d_{k,l} = s_l
Theorem mx1: d_{i,j} +? d_{k,l} ==> d_{i,j} ++ d_{k,l} = d_{i,l}


範囲メタデータモデル(level2: apply, merge)

md ++? md' := md.B.B <= md'.B.B <= md.E.B < md'.E.B
md +++ md'
  := md.B-->|md'.E.B, max(md'.E.B, md.E.E)| if md' is clean
     md.B-->md'.E                           otherwise
ms <<? md := md.B.B <= ms.B < md.E.B
ms <<< md := |md.E.B, max(md.E.B, ms.E)| if md is clean
             md.E                        otherwise
md_{i,j} := md_i +++ md_{i+1} +++ ... +++ md_{j-1}

このとき
md_{i,j}.B = md_i.B = ms_i.B
md_{i,j}.E.B = md_{j-1}.E.B = ms_j.B
が成り立つ.

Theorem rx1: md_i ++? md_j <==> d_i +? d_j
Theorem rx2: ms_i <<? md_j <==> s_i <? d_j

Theorem rx3: md_{i,j} ++? md_{k,l} <==> d_{i,j} +? d_{k,l}
Theorem rx4: ms_i <<? md_{k,l} <==> s_i <? d_{k,l}

Theorem rx5a: i < j ==> ms_i.E <= ms_j.E

Lemma rx5b: md_{i,j} と ms_i, ms_j の関係
Theorem rx5c: md_{i,j} ++? md_{k,l} ==> md_{i,j} +++ md_{k,l} = md_{i,l}
Theorem rx5: ms_i <<? md_{k,l} ==> ms_i <<< md_{k,l} = ms_l
Theorem rx5d: ms_i <<? md_{k,l} and md_{k,l} ++? md_{m,n} ==> ms_i <<< md_{k,l} <<< md_{m,n} = ms_i <<< md_{k,l} +++ md_{m,n}

Theorem rx6: M2B(md_i) = d_i, M2B(ms_i) = s_i
Theorem rx7: M2B(md_{i,j}) = d_{i,j}
Theorem rx8: ms_i <<? md_{k,l} ==> M2B(ms_i <<< md_{k,l}) = M2B(ms_i) << M2B(md_{k,l})
```


#### Theorem rx1: `md_i ++? md_j <==> d_i +? d_j`

証明

```
d_i +? d_j が true
  <=> i <= j <= i + 1 < j + 1
  <=> j = i + 1

md_i ++? md_j が true
  <=> md_i.B.B <= md_j.B.B <= md_i.E.B < md_j.E.B

i + 1 == j のとき，

md_i ++? md_j
  = md_i.B.B <= md_{i+1}.B.B <= md_i.E.B < md_{i+1}.E.B
  = True and True and True
  = True

i + 1 < j のとき

md_i ++? md_j
  = True and False and True
  = False

i == j のとき

md_i ++? md_j
  = True and True and False
  = False

i > j のとき

md_i ++? md_j
  = False and True and False
  = False

よって，示された．
```


#### Theorem rx2: `ms_i <<? md_j <==> s_i <? d_j`

証明

```
s_i <? d_j が true
  <=> j <= i < j+1
  <=> i = j

ms_i <<? md_j が true
  <=> md_j.B.B <= ms_i.B < md_j.E.B

i == j のとき

ms_i <<? md_j
  = True and True
  = True

i < j のとき

ms_i <<? md_j
  = False and True
  = False

i > j のとき

ms_i <<? md_j
  = True and False
  = False

よって，示された．
```


#### Theorem rx3: `md_{i,j} ++? md_{k,l} <==> d_{i,j} +? d_{k,l}`

証明

```
d_{i,j} +? d_{k,l} が true
  <=> i <= k <= j < l

md_{i,j} ++? md_{k,l} が true
  <=> md_{i,j}.B.B <= md_{k,l}.B.B <= md_{i,j}.E.B < md_{k,l}.E.B
  <=> ms_i.B <= ms_k.B <= ms_j.B < ms_l.B

ms_i.B は i が増えるにつれて単調増加するため，
(i <= k <= j < l) <=> (ms_i.B <= ms_k.B <= ms_j.B < ms_l.B)

よって，示された．
```


#### Theorem rx4: `ms_i <<? md_{k,l} <==> s_i <? d_{k,l}`

証明
```
s_i <? d_{k,l} が true
  <=> k <= i < l

ms_i <<? md_{k,l} が true
  <=> md_{k,l}.B.B <= ms_i.B < md_{k,l}.E.B
  <=> ms_k.B <= ms_i.B < ms_l.B

ms_i.B は i が増えるにつれて単調増加するため，
(k <= i < l) <=> (ms_k.B <= ms_i.B < ms_l.B)

よって，示された．
```

#### Theorem rx5a: `i < j ==> s_i.E <= s_j.E`

証明

```
ルール(1) のとき

ms_{i+1}
  = ms_i <<< md_i
  = |B', max(B', ms_i.E)|
ms_{i+1}.E
  = max(B', ms_i.E)
  >= ms_i.E

ルール(2) のとき

ms_{i+1} = md_i.E = E' > B' > ms_i.E

よって示された．
```

#### Lemma rx5b: `md_{i,j} と ms_i, ms_j の関係`

```
md_{i,j} の中で，
i < k <= j であるような md_{k-1} および ms_k について，
ルール(2) で構成されている最大の k について考える．
ただし，そのような k が存在しない場合，k = i とする．

Lemma rx5b1

k = i のとき，
  md_{i,j} = |ms_k.B|-->|md_{j-1}.E.B|
  ms_j = |md_{j-1}.E.B, max(md_{j-1}.E.B, ms_k.E)|

k > i のとき，
  md_{i,j} = md_{i,j}.B-->|md_{j-1}.E.B, max(md_{j-1}.E.B, ms_k.E)|
  ms_j = md_{i,j}.E
    ただし，md_{i,j}.B は |md_i.B.B| もしくは md_i.B である．
    (md_i の構成方法がルール(1) かルール(2) かに依存)．

証明

k = i のとき

md_i, md_{i+1}, ..., md_{j-1} が全てルール (1) で構成されている．
構成方法の定義から，
md_{i,j} = |ms_k.B|-->|md_{j-1}.E.B|
これは clean diff なので，apply の定義により，
ms_j
  = ms_i <<< md_{i,j}
  = |md_{j-1}.E.B, max(md_{j-1}.E.B, ms_i.E)|
  = |md_{j-1}.E.B, max(md_{j-1}.E.B, ms_k.E)|


k > i のとき

md_{k-1} と ms_k が ルール(2) で，
md_k, ... md_{j-1} は，ルール(1) で構成されたことになる．
すなわち，
md_{k,j} = |ms_k.B|-->|md_{j-1}.E.B|
また，md_{i,k} = md_{i,k}.B-->ms_k
merge の定義により，
md_{i,j}
  = md_{i,k} +++ md_{k,j}
  = md_{i,k}.B-->|md_{j-1}.E.B, max(md_{j-1}.E.B, ms_k.E)|
  = md_{i,j}.B-->|md_{j-1}.E.B, max(md_{j-1}.E.B, ms_k.E)|

md_{k,j} は clean なので，apply の定義により，
ms_j
  = ms_k <<< md_{k,j}
  = |md_{j-1}.E.B, max(md_{j-1}.E.B, ms_k.E)|
  = md_{i,j}.E

よって，示された．


Lemma rx5b2

i <= k <= l <= j であるような任意の l に対して，
ms_k.E > md_{j-1}.E.B のとき，
  ms_l = |md_{l-1}.E.B, ms_k.E|
ms_k.E <= md_{j-1}.E.B のとき，
  ms_l = |md_{l-1}.E.B, ms_l.E|
  ms_l.E = ms_k.E if ms_l.B < ms_k.E
           ms_l.B otherwise

証明

ms_k.E > md_{j-1}.E.B のとき，
k <= l <= j である任意の l に対して，
md_{l-1}.E.B <= md_{j-1}.E.B < ms_k.E であるため，
ms_l = |md_{l-1}.E.B, ms_k.E|

ms_k.E <= md_{j-1}.E.B のとき，
k <= l <= j であるような任意の l に対して，
apply の定義により，
ms_l
  = ms_k <<< md_{k,l}
  = |md_{l-1}.E.B, max(md_{l-1}.E.B, ms_k.E)|
  = |ms_l.B, max(ms_l.B, ms_k.E)|
  ms_l.B < ms_k.E のとき
    = |ms_l.B, ms_k.E|
    よって ms_l.E = ms_k.E
  ms_l.B >= ms_k.E のとき，
    = |ms_l.B|
    よって ms_l.E = ms_l.B

よって，示された．
```


#### Theorem rx5c: `md_{i,j} ++? md_{k,l} ==> md_{i,j} +++ md_{k,l} = md_{i,l}`

結合法則。

証明

```
merge 条件より，i <= k <= j < l．
Lemma rx5b を用いて (a)(b)(c)(d) の 4 通りに場合分けする．


(a) md_{i,j} および md_{k,l} が全てルール(1) で構成されている場合

md_{i,j} = |md_i.B.B|-->|md_{j-1}.E.B|
md_{k,l} = |md_k.B.B|-->|md_{l-1}.E.B|
md_{i,j} +++ md_{k,l}
  = |md_i.B.B|-->|md_{l-1}.E.B|

このとき，md_{i,l} も全てルール(1) で構成される．
md_{i,l} = |md_i.B.B|-->|md_{l-1}.E.B|

よって md_{i,j} +++ md_{k,l} = md_{i,l}


(b) md_{i,j} が全てルール(1) で，md_{k,l} はルール(2) を含む場合

md_{k,l} 内の最後の dirty diff を md_{n-1} とすると，
k < n <= l．

md_{i,j} = |md_i.B.B|-->|md_{j-1}.E.B|
md_{k,l} = md_{k,l}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_n.E)|
md_{i,j} +++ md_{k,l}
  if md_{k,l} is clean (==> md_{l-1}.E.B >= ms_n.E)
    = |md_i.B.B|-->|md_{l-1}.E.B|
  else
    = |md_i.B.B|-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_n.E)|
    = |md_i.B.B|-->|md_{l-1}.E.B, ms_n.E|

このとき，md_{i,l} において，最後の dirty diff は md_{n-1}．

md_{i,l}
  = md_{i,l}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_n.E)|
  = |md_i.B.B|-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_n.E)|

よって，md_{i,j} +++ md_{k,l} = md_{i,l}


(c) md_{i,j} がルール(2) を含み，md_{k,l} は全てルール(1) である場合

md_{i,j} 内の最後の dirty diff を md_{m-1} とすると，
i < m <= j．

md_{i,j} = md_{i,j}.B-->|md_{j-1}.E.B, max(md_{j-1}.E.B, ms_m.E)|
md_{k,l} = |md_k.B.B|-->|md_{l-1}.E.B|

md_{i,j} +++ md_{k,l}
  = md_{i,j}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, md_{j-1}.E.B, ms_m.E)|
  = md_{i,l}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_m.E)|

md_{i,l} において，最後の dirty diff は md_{m-1}．

md_{i,l}
  = md_{i,l}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_m.E)|

よって，md_{i,j} +++ md_{k,l} = md_{i,l}


(d) md_{i,j} および md_{k,l} が両方ともルール(2) を含む場合

md_{i,j} 内の最後の dirty diff を md_{m-1}，
md_{k,l} 内の最後の dirty diff を md_{n-1} とすると，
i < m <= j, k < n <= l．

md_{i,j} = md_{i,j}.B-->|md_{j-1}.E.B, max(md_{j-1}.E.B, ms_m.E)|
md_{k,l} = md_{k,l}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_n.E)|

md_{i,j} +++ md_{k,l}
  if md_{k,l} is clean (then md_{l-1}.E.B >= ms_n.E)
    = md_{i,j}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, md_{j-1}.E.B, ms_m.E)|
    md_{l-1}.E.B >= ms_n.E > ms_m.E であるから，
    = md_{i,l}.B-->|md_{l-1}.E.B|
  else
    = md_{i,j}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_n.E)|
    = md_{i,l}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_n.E)|

md_{i,l} 内の最後の dirty diff は md_{n-1}．

md_{i,l}
  = md_{i,l}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_n.E)|

よって，md_{i,j} +++ md_{k,l} = md_{i,l}


(a)(b)(c)(d) 全ての場合について示されたので，
md_{i,j} +++ md_{k,l} = md_{i,l}
```


#### Theorem rx5: `ms_i <<? md_{k,l} ==> ms_i <<< md_{k,l} = ms_l`

証明

```
ms_i <<? md_{k,l} <=> k <= i < l

Lemma rx5b より，

md_{k,l} に含まれる全 md_i k <= i < l がルール(1) のみで構成される場合，

md_{k,l} = |md_k.B.B|-->|md_{l-1}.E.B|

ms_i <<< md_{k,l}
  = |md_{l-1}.E.B, max(md_{l-1}.E.B, ms_i.E)|

ms_l = |md_{l-1}.E.B, max(md_{l-1}.E.B, ms_k.E)|

md_{l-1}.E.B < ms_k.E のとき，
Lemma rx5b2 より、ms_i = |md_{i-1}.E.B, ms_k.E|
よって、ms_i.E = ms_k.E

md_{l-1}.E.B >= ms_k.E のとき，
  ms_i.B < ms_k.E のとき
    ms_i.E = ms_k.E
  ms_i.B >= ms_k.E のとき
    ms_i.E = ms_i.B <= md_{l-1}.E.B
    ms_i <<< md_{k,l} = ms_l = |md_{l-1}.E.B|

よって，ms_i <<< md_{k,l} = ms_l


md_{k,l} に含まれる md_i k <= i < l において，ルール(2) で構成されるものが含まれる場合，
そのような最後の md を md_{m-1} とする．k < m <= l．

md_{k,l} = md_{k,l}.B-->|md_{l-1}.E.B, max(md_{l-1}.E.B, ms_m.E)|

ms_i <<< md_{k,l}
  if md_{k,l} is clean (then md_{l-1}.E.B >= ms_m.E)
    = |md_{l-1}.E.B, max(md_{l-1}.E.B, ms_i.E)|
  else
    = |md_{l-1}.E.B, max(md_{l-1}.E.B, ms_m.E)|

ms_l = |md_{l-1}.E.B, max(md_{l-1}.E.B, ms_m.E)|


md_{k,l} is clean のとき，
  i <= m の場合，
    ms_i.E <= ms_m.E <= md_{l-1}.E.B なので，
    ms_i <<< md_{k,l} = ms_l = |md_{l-1}.E.B|
  m < i の場合，
    ms_i.B < ms_m.E の場合
      ms_i.E = ms_m.E
      よって，ms_i <<< md_{k,l} = ms_l
    ms_i.B >= ms_m.E の場合
      ms_i.E = ms_i.B <= ms_l.B = md_{l-1}.E.B
      よって，ms_i <<< md_{k,l} = ms_l = |md_{l-1}.E.B|
else
  明らかに ms_i << md_{k,l} = ms_l

以上，全ての場合において ms_i <<< md_{k,l} = ms_l が示された．
```


#### Theorem rx5d: `ms_i <<? md_{k,l} and md_{k,l} ++? md_{m,n} ==> ms_i <<< md_{k,l} <<< md_{m,n} = ms_i <<< md_{k,l} +++ md_{m,n}`

merge してから apply しても，順に apply しても結果は同じ法則．

証明

```
ms_i <<? md_{k,l} <=> k <= i < l
md_{k,l} ++? md_{m,n} <=> k <= m <= l < n

これらから，k <= l < n が成立する．
k <= l < n <=> ms_l <<? md_{m,n}

同様に、k <= i < n も成立する。
k <= i < n <=> ms_i <<? md_{k,n}

Theorem rx5 より，
ms_i <<< md_{k,l} <<< md_{m,n}
  = ms_l <<< md_{m,n}
  = ms_n

Theorem rx5c より，
md_{k,l} +++ md_{m,n} = md_{k,n}

ms_i <<< md_{k,l} +++ md_{m,n}
  = ms_i <<< md_{k,n}
  = ms_n

よって，示された．
```


#### Theorem rx6: `M2B(md_i) = d_i, M2B(ms_i) = s_i`

Theorem ry2 で証明済み．


#### Theorem rx7: `M2B(md_{i,j}) = d_{i,j}`

証明

```
Theorem rx6 より，
M2B(md_i) = d_i for all i

M2B(md_{i,j})
  = M2B(md_i) ++ M2B(md_{i+1}) ++ ... ++ M2B(md_{j-1})
  = d_i ++ d_{i+1} ++ ... ++ md_{j-1}
  = d_{i,j}

よって示された．
```

#### Theorem rx8: `ms_i <<? md_{k,l} ==> M2B(ms_i <<< md_{k,l}) = M2B(ms_i) << M2B(md_{k,l})`

証明

```
Theorem rx5 より，
ms_i <<< md_{k,l} = ms_l

Theorem rx4 より，
ms_i <<? md_{k,l} <==> s_i <? d_{k,l}

Theorem rx6 より，
M2B(ms_l) = s_l
M2B(ms_i) = s_i

Theorem rx7 より，
M2B(md_{k,l}) = d_{k,l}

M2B(ms_i) << M2B(md_{k,l})
  = s_i << d_{k,l}
Theorem m8 より，
  = s_l

故に，示された．
```


### Level3: apply, merge, compared_diff

```
単純メタデータモデル(level3: apply, merge, compared_diff)

追加の定義

i < j について
d_{i:j} := compared_diff(s_i, s_j)
d_{i:j} +? d_{k,l} := False
d_{i,j} +? d_{k:l} := False
d_{i:j} +? d_{k:l} := False
d_{i:j} について ++ は定義しない．
s_i <? d_{k:l} := k == i
d_{i~j} を d_{i,j} もしくは d_{i:j} とする．

追加の証明

Theorem m8: s_i <? d_{k,l} ==> s_i << d_{k,l} = s_l
を拡張して，
Theorem mx8a: d = d_{k~l} のとき s_i <? d ==> s_i << d = s_l
を示す．

Theorem mx1: d_{i,j} +? d_{k,l} ==> d_{i,j} ++ d_{k,l} = d_{i,l}
は Theorem mx2 として拡張．ただし，d_{i:j} や d_{k:l} は
merge 不可なので，ほとんど同じ．


範囲メタデータモデル(level3: apply, merge, compared_diff)

追加の定義

i < j として，md_{i:j} := ms_i-->md_j
md_{i:j} ++? md_{k,l} := False
md_{i,j} ++? md_{k:l} := False
md_{i:j} ++? md_{k:l} := False
md_{i:j} について ++ は定義しない．
ms <<? md_{i:j} := ms == md_{i:j}.B
ms <<< md_{i:j} := これまでの ms <<< md をそのまま使う
md_{i~j} を md_{i,j} もしくは md_{i:j} とする．
M2B(md_{i:j}) := compared_diff(s_i, s_j)


追加の証明

Theorem rx1: md_i ++? md_j <==> d_i +? d_j
変更の必要なし

Theorem rx2: ms_i <<? md_j <==> s_i <? d_j
変更の必要なし

Theorem rz3: mdx ++? mdy <==> dx +? dy
ただし
mdx, dx = md_{i,j}, d_{i,j} または md_{i:j}, d_{i:j}
mdy, dy = md_{k,l}, d_{k,l} または md_{k:l}, d_{k:l}
とする．

Theorem rz4a: ms_i <<? md_{k:l} <==> s_i <? d_{k:l}
を示せば，Theorem rx4 と合わせて
Theorem rz4: ms_i <<? md <==> s_i <? d
ただし md, d = md_{k,l}, d_{k,l} または md_{k:l}, d_{k:l}
が示される．

Theorem rz5a: ms_i <<? md_{k:l} ==> ms_i <<< md_{k:l} = ms_l
を示せば，Theorem rx5 と合わせて
Theorem rz5: ms_i <<? md ==> ms_i <<< md = ms_l
ただし，md = md_{k,l} または md_{k:l}
が示される．

Theorem rx5c: md_{i,j} ++? md_{k,l} ==> md_{i,j} +++ md_{k,l} = md_{i,l}
Theorem rx5d: ms_i <<? md_{k,l} and md_{k,l} ++? md_{m,n} ==> ms_i <<< md_{k,l} <<< md_{m,n} = ms_i <<< md_{k,l} +++ md_{m,n}
は変更の必要なし．md_{i:j} は merge できないから．

Theorem rx6: M2B(md_i) = d_i, M2B(ms_i) = s_i
変更の必要なし．

Theorem rz7a: M2B(md_{i:j}) = d_{i:j}
を示せば，Theorem rx7 と合わせて
Theorem rz7: M2B() = d_{i,j}
ただし md, d = md_{i,j}, d_{i,j} または md_{k:l}, d_{k:l}
が示される．

Theorem rz8a: ms_i <<? md_{k:l} ==> M2B(ms_i <<< md_{k:l}) = M2B(ms_i) << M2B(md_{k:l})
を示せば，Theorem rx8 と合わせて
Theorem rz8: ms <<? md ==> M2B(ms <<< md) = M2B(ms) << M2B(md)
が示される．

```

#### Theorem rz3: `dx ++? mdy <==> dx +? dy`

ただし
```
mdx, dx = md_{i,j}, d_{i,j} または md_{i:j}, d_{i:j}
mdy, dy = md_{k,l}, d_{k,l} または md_{k:l}, d_{k:l}
```
とする．

証明

```
mdx, dx = md_{i,j}, d_{i,j},
mdy, dy = md_{i,j}, d_{i,j} のとき

Theorem rx3 にて示した．

その他のとき，

mdx ++? mdy = False
dx +? dy = False

よって，示された．
```


#### Theorem rz4a: `ms_i <<? md_{k:l} <==> s_i <? d_{k:l}`

証明

```
s_i <? d_{k:l}
  <=> k == i

ms_i <<? md_{k:l}
  <=> ms_i == md_{k:l}.B
  <=> ms_i == md_k
  <=> k == i

よって示された．
```


#### Theorem rz5a: `ms_i <<? md_{k:l} ==> ms_i <<< md_{k:l} = ms_l`

証明

```
Theorem rz4a より，
ms_i <<? md_{k:l} <==> k == i

ms_i <<< md_{k:l}
  = ms_i <<< md_{i:l}
  = ms_l

よって示された．
```


#### Theorem rz7a: `M2B(md_{i:j}) = d_{i:j}`

証明

```
M2B(md_{i:j})
  = compared_diff(M2B(ms_i), M2B(ms_j))
Theorem rx6 より
  = compared_diff(s_i, s_j)
  = d_{i:j}

よって，示された．
```


#### Theorem rz8a: `ms_i <<? md_{k:l} ==> M2B(ms_i <<< md_{k:l}) = M2B(ms_i) << M2B(md_{k:l})`

証明

```
Theorem rz4a より
ms_i <<? md_{k:l} <==> k == i

M2B(ms_i <<< md_{k:l})
  = M2B(ms_l)
  = s_l

M2B(ms_i) << M2B(md_{k:l})
Theorem rz7a より
  = s_i << d_{k:l}
  = s_i << d_{i:l}
  = s_l

よって，示された．
```


### Level4: apply, merge, compared_diff, applying

merge と apply の同時実行を禁止する仕様では，このモデルは不要．


-----
