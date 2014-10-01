# walb-tools apply/merge 再再考 (r20140930a)

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


### 定理 b1: `(d0 ++ d1) ++ d2 = d0 ++ (d1 ++ d2)`

merge 操作は結合法則が成り立つ．

証明

```
d01 = d0 ++ d1 と置く．

d01[a] = d1[a] if a in |d1|
         d0[a] if a in |d0|-|d1|

(d01 ++ d2)[a] =  d2[a] if a in |d2|
                 d01[a] if a in |d01|-|d2|
               = d2[a] if a in |d2|
                 d1[a] if a in |d1|-|d2|
                 d0[a] if a in |d0|-|d1|-|d2|

d12 = d1 ++ d2 と置く．

d12[a] = d2[a] if a in |d2|
         d1[a] if a in |d1|-|d2|

(d0 ++ d12)[a] = d12[a] if a in |d12|
                  d0[a] if a in |d0|-|d12|
               = d2[a] if a in |d2|
                 d1[a] if a in |d1|-|d2|
                 d0[a] if a in |d0|-|d1|-|d2|

故に (d0 ++ d1) ++ d2 = d0 ++ (d1 ++ d2)
```

### 定理 b2: `s << d0 << d1 = s << d0 ++ d1`

d0 と d1 を 順に apply する操作と，d0 と d1 を merge 後に apply する操作は結果が等しい．

証明

```
左辺を s0，右辺を s1 と置く．

s00 = s << d0 と置く．

s00[a] = d0[a] if a in |d0|
          s[a] if a not in |d0|

s0[a] =  d1[a] if a in |d1|
        s00[a] otherwise
      = d1[a]  if a in |d1|
        d0[a]  if a in |d0|-|d1|
         s[a]  otherwise

dx = d0 ++ d1 と置く．

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

`compared_diff(s0, s1) = compared_diff(s1, s0)` は `s0 = s1` のときのみ成り立つのは明らかで，
このとき empty diff となる．

(walb の実装においては，直接ブロックを比較するのではなく
 hash 値を用いることで compared diff 構成の際に発生する転送データを減らす．)


### 定理 b3: dirty_snapshot(tx0,tx1) << log_diff(ty0,ty1) = dirty_snapshot(ty1,max(ty1,tx1))

ただし，ty0 <= tx0 and tx0 < ty1 を満たすものとする．

証明

```
s = dirty_snapshot(tx0, tx1)，
d = log_diff(ty0, ty1)
s' = s << d
と置く．

任意の a に対して，
s[a] = b[a][t] (tx0 <= t <= tx1)

T_a is empty のとき，
write IO は [ty0, ty1] 区間で発生しなかったので，
b[a][t] s.t. ty0 <= t <= ty1 は全て等しい．
これと，ty0 <= tx0 and tx0 < ty1 から，
ty1 >= tx1 のとき
  s[a] = b[a][ty1]
ty1 < tx1 のとき
  s[a] = b[a][t] s.t. ty1 <= t <= tx1

T_a is not empty のとき，
log diff の定義より，

d[a] = b[a][max(T_a)] = b[a][ty1]

apply の定義により，

s'[a] = d[a] if a in |d|
        s[a] otherwise

ty1 >= tx1 のとき，
s'[a] = b[a][ty1] if a in |d|
        b[a][ty1] otherwise
      = b[a][ty1]

ty1 < tx1 のとき，
s'[a] = b[a][ty1]                    if a in |d|
        b[a][t] s.t. ty1 <= t <= tx1 otherwise
      = b[a][t] s.t. ty1 <= t <= tx1

よって，
s' = dirty_snapshot(ty1, max(ty1, tx1))
故に，示された．
```

より単純なケースでは，以下が成立する．
```
t0 <= t1 <= t2 <= t3 のとき，
dirty_snapshot(t1,t2) << log_diff(t0,t3) = clean_snapshot(t3)
```


### 定理 b4: s << compared_diff(s, s') = s'

証明

```
d = compared_diff(s, s') と置く．

compared_diff の定義により，任意の a について，
d[a] = s'[a] if s[a] != s'[a]
       null  otherwise

(s << d)[a] = s'[a] if s[a] != s'[a]
              s[a]  if s[a] == s'[a]
            = s'[a]

よって，示された．
```

## 単純メタデータモデル

### 定義

- ある block device b の snapshot s0 と、
  diff 列 d0, d1, ... が与えられたとき、
- snapshot s1, s2, ... を、apply を用いて以下のように構成する。

```
s0 << d0 = s1
s1 << d1 = s2
...
s_i << d_i = s_{i+1}
```

#### merged diff `d_{i,j}`

diff 列の連続する部分列を merge したものを `d_{i,j}` と書く．

```
d_{i,j} := d_i ++ d_{i+1} ++ ... ++ d_{j-1}  (i < j)
           empty diff                        (i = j)
```

#### compared diff `d_{i:j}`

`s_i` と `s_j` から作られた compared diff を，`d_{i:j}` と書く．

```
d_{i:j} := compared_diff(s_i, s_j)  (i < j)
           empty diff               (i = j)
```

ただし，
```
d_{i:i+1} = d_i
```
として問題ないことに注意．

証明

```
dx = d_{i:i+1} と置く．

dx = compared_diff(s_i, s_{i+1}) より，任意の a について，

|dx| = {a | s_i[a] != s_{i+1}[a]}
dx[a] = s_{i+1}[a] if a in |dx|
a not in |dx| ==> s_i[a] = s_{i+1}[a]

s' = s_i << d_{i:i+1} と置く．

apply の定義により，

s'[a] = dx[a]  if a in |dx|
        s_i[a] otherwise
      = s_{i+1}[a] if a in |dx|
        s_{i+1}[a] otherwise
      = s_{i+1}[a]

s' = s_{i+1}
すなわち，

s_i << d_{i:i+1} = s_{i+1}
```

#### canMerge predicate

merged diff もしくは compared diff が 2 つ (`d0` と `d1`) あったとき，
以下の条件を満たす述語 canMerge を定義し，`canMerge(d0, d1)` とか `d0 +? d1` と書く．
ただし、`d0` と `d1` の diff の種類によって 4 通りに分けられる。

```
(1) canMerge(d_{i0,j0}, d_{i1,j1}) := i1 <= j0 and i0 <= j1
(2) canMerge(d_{i0:j0}, d_{i1,j1}) := i1 <= j0 and i0 <= j1 and i0 + 1 == j0
(3) canMerge(d_{i0,j0}, d_{i1:j1}) := i1 <= j0 and i0 <= j1 and i1 + 1 == j1
(4) canMerge(d_{i0:j0}, d_{i1:j1}) := i0 + 1 == j0 and i1 + 1 == j1
```

(1) は merge 後の diff が diff 列の連続部分列となる条件であることを意味する．
(2)(3)(4) は，compared diff は `d_{i:i+1}` のみ merged diff と同等に扱って良いことを意味する．


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
(1) canApply(s_(i,j), d_{k,l}) := i >= k and j <= l
(2) canApply(s_(i,j), d_{k:l}) :=
  if i == j のとき
    i <= l
  otherwise
    i == k and j == l
```

(2) に関して、`i == j` の時の方が、apply の適用範囲が広い。


#### 重複のない diff 列

重複のない diff 列を，以下のルールで生成された diff 列とする．

- diff 列が空の場合は，`d0, d_{0,i}, d_{0:i}` のいずれかを加える `(i > 0)`．
- diff 列が空でない場合は，その最後の diff `d_{j-1}, d_{i,j}, d_{i:j}` のいずれかに対して，
  `d_j, d_{j,k}, d_{j:k}` のいずれかを diff 列の最後に追加する `(j < k)`．

例: `d0, d1, d_{2:5}, d5, d_{6,8}, d_{8:10}, d10, d11`


### 補題＆定理

まとめ

- Lemma m1: `a in |d_{i,j}| ==> d_{i,j}[a] = s_j[a]`
- Lemma m2: `a in |d_{i:j}| ==> d_{i:j}[a] = s_j[a]`

- Lemma m12: `d = d_(i,j) のとき a in |d| ==> d[a] = s_j[a]`

- Lemma m3: `s_i << d_{j,i} = s_i`
- Lemma m4: `s_{i,j} << d_{k,i} = s_{i,j}`
- Lemma m5: `a not in |d_{i,j}| ==> a not in |d_{i:j}|`
- Lemma m6: `a not in |d_{i,j}| ==> a not in |d_{i',j'}| (i <= i', j' <= j)`
- Lemma m7: `s = s_{i:j} ==> s = s_{i,j}`

- Theorem  m8: `s_i <? d_{k,l} ==> s_i << d_{k,l} = s_l`
- Theorem  m9: `s_i <? d_{k:l} ==> s_i << d_{k:l} = s_l`
- Theorem m10: `s_i <? d_{k,l} ==> s_i <: d_{k,l} = s_{i:l}`
- Theorem m11: `s_i <? d_{k:l} ==> s_i <: d_{k:l} = s_{i:l}`
- Theorem m12: `s_{i,j} <? d_{k,l} ==> s_{i,j} << d_{k,l} = s_l`
- Theorem m13: `s_{i:j} <? d_{k:l} ==> s_{i:j} << d_{k:l} = s_l`
- Theorem m14: `s_{i,j} <? d_{k,l} ==> s_{i,j} <: d_{k,l} = s_{i,l}`
- Theorem m15: `s_{i:j} <? d_{k:l} ==> s_{i:j} <: d_{k:l} = s_{i:l}`
- Theorem m  :

s = s_(i,j) <? d_(k,l) ==>

- Theorem m16: apply 可能 diff の存在
- Theorem m17: 重複のない diff 列において，s_{i,j} か s_{i:j} のどちらかしか存在しない


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


#### Theorem  m8: `s_i <? d_{j,k} ==> s_i << d_{j,k} = s_k`

証明

```
canApply の定義により，j <= i <= k．

s_i << d_{j,k} = s_i << d_{j,i} ++ d_{i,k}
               = s_i << d_{j,i} << d_{i,k}
Lemma m3 より
               = s_i << d_{i,k}
               = s_k
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

sx = s_i <: d_{j,k} と置く．

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

sx = s_i <: d_{i:k} と置く．

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

sx = s_{i,j} << d_{k,l} と置く．

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

sx = s_{i:j} << d_{i:j} と置く．

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

sx = s_{i,j} <: d_{k,l} と置く．

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

sx = s_{i:j} <: d_{i:j} と置く．

s_{i:j} の定義より，

s_{i:j}[a] = s_i[a] or s_j[a]

applying の定義より，

sx[a] = s_{i:j}[a] or d_{i:j}[a] if a in |d_{i:j}|
        s_{i:j}[a]               otherwise
      = s_{i:j}[a]

故に，sx = s_{i:j} = s_{i:l}．
よって，示された．
```


#### Theorem m16: apply 可能 diff の存在

重複のない diff 列が与えられたものとする．
(例: `d0, d_{1:3}, d3, d_{4:8}, d8, d9, ...`)

この diff 列は，時間経過と共に，canMerge によって許されている任意の diff の組の部分集合を
merge し，merge 前の diff を置き換えるものとする．
(例: `d8` と `d9` を merge して `d_{8,10}` で置き換える)

時刻 `t0` において，`s_i` に対して，apply 可能な `d_{j,k}` または `d_{j:k}` を適用しようとし，
applying 操作によって，`s_{i,k}` または `s_{i:k} が得られたものとする．
その後，どのように merge が実行され diff 列内の diff が置き換えられたとしても，
時刻 `t1 (t0 < t1)` において，`s_{i,k}` や `s_{i:k}` に対して apply 可能な diff が必ず存在することを示す．

証明

```
case 1: d_{j,k} の場合

canApply の定義により，j <= i <= k (1)

canMerge の定義により，列に存在する compared diff は merge 出来ないため，
[t0,t1] の間にどのように merge が実行されようとも，
d_{j,k} を含む diff d_{j',k'} s.t. (j' <= j, k <= k') が t1 において存在する．(2)

canApply の定義により，
s_{i,k} <? d_{j',k'} = i >= j' and k <= k' (3)

(1) と (2) より j' <= j <= i であるから i >= j'
(2) より k <= k'
よって，(3) は True となる．

故に s_{i,k} に apply 可能な diff が t1 において存在する．


case 2: d_{j:k} の場合

canApply の定義により，i = j (4)

前提により，列に存在する d_{j:k} は j + 1 < k であるため，
canMerge の定義から，他の diff と merge 出来ない．
故に，[t0, t1] の間にどのような merge が実行されようとも，
t0 の時点で列に存在した d_{j:k} は t1 においても存在する．

よって，

s_{i,k} <? d_{j:k} = i == j and k == k (5)

(4) により (5) は True となる．
よって s_{i:k} に apply 可能な diff が t1 において存在する．

以上，全ての場合において成立することが示された．
```


#### Theorem m17: 重複のない diff 列において，s_{i,j} か s_{i:j} のどちらかしか存在しない

Theorem m16 と同様に，重複のない diff 列が与えられ，
時間経過と共に canMerge(dx, dy) が許される diff dx, dy がそれらを merge した diff で
置き換えられていく状況を考える．



QQQ


### log diff と hash diff 違いが apply に及ぼす影響の考察

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

d.is_compared := True または False
d.is_dirty := (d.B.is_clean and d.E.is_clean)
d.is_clean := not d.is_dirty
```

log 由来の diff d は `d.is_compared = False`，
hash-bkp 由来の diff d は `d.is_compared = False`，
hash-repl 由来の diff d は，`d.is_compared = True` とする．

`d.is_clean` が `True` のとき，clean diff と呼び，
`False` のとき，dirty diff と呼ぶ．


#### canMergeR

canMergeR (演算子 `++?`) を次のように定義する．

```
d0 ++? d1 := d0.B.B <= d1.B.B and d0.E.B < d1.E.B and d1.B.B <= d0.E.B
             and not d0.is_compared
             and not d1.is_compared
```

それぞれの条件は以下を意味する．
- `d0.B.B <= d1.B.B` d0 の方がより古い snapshot に適用できること
- `d0.B.B < d1.B.B` d1 の方がより新しい snapshot を生成すること
- `d1.B.B <= d0.E.B` d0 と d1 は重複もしくは連続していること


#### mergeR

mergeR (演算子 `+++`) を次のように定義する．

```
d0 +++ d1 := d0.B-->|d1.E.B,max(d0.E.E,d1.E.B)| if d0.E.is_dirty and d1.is_clean
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
s <<? d := d.B.B <= s.B and s.B < d.E.B
as <<? d := d.B.B <= as.B.B and as.B.B < d.E.B and as.E.B <= d.E.B
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

  ms_i に対して，md_i := |B|-->|B'| とする．
  ただし，B = ms_i.B < B' とする．
  その後，ms_{i+1} := ms_i <<< md_i とする．

  同様に，d_i := log_diff(B, B')，
  s_{i+1} := s_i << d_i とする．

ルール(2)

  ms_i に対して，ms_{i+1} := |B',E'| とする．
  ただし，ms_i.B < B' とする．
  その後，md_i := ms_i-->ms_{i+1} とする．

  同様に，s_{i+1} := dirty_snapshot(B',E')，
  d_i := compared_diff(s_i, s_{i+1}) とする．

```

上記から任意の `i` について `s_i.B = d_i.B.B < d_i.E.B = s_{i+1}.B` が成り立つ．

以下のように変換 M2B を定義する．
```
M2B(ms_i) := {dirty_snapshot(ms_i.B, ms_i.E)}

M2B(md_i) := log_diff(md_i.B.B, md_i.E.B) if md_i.is_clean
             compared_diff(s_i, s_{i+1})  otherwise
```


### 補題＆定理

まとめ

- Theorem: `for all i, j ms_i <<? md_j <==> i == j (<==> s_i <? d_j)`
- Theorem: `for all i M2B(md_i) = d_i, M2S(ms_i) = s_i


#### Theorem: `for all i, j ms_i <<? md_j <==> i == j (<==> s_i <? d_j)`

証明

```
B_i = s_i.B と置く．

progress rule より B_i < B_j <==> i < j

pred = ms_i <<? md_j と置く．

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


#### Theorem: `for all i M2B(md_i) = d_i, M2S(ms_i) = s_i

QQQ



-----


以下は，未整理のメモ．


## snapshot と diff の定義

log diff と hash diff には性質に違いがある．
ある addr における bdata について考える．
時刻 t0 のときは A で，t1 のときは B だったとする．

(1) 当該ブロックに対して write IO が発生しなかった場合:
  A == B であり，log/hash diff 共に当該ブロックのデータは含まない．

(2) 当該ブロックに対して write IO が発生し，A != B の場合:
  log/hash diff 共に当該ブロックのデータとして B を含む．

(3) 当該ブロックに対して write IO が発生し，A == B の場合:
  log diff の場合 B(==A) を含むが，hash diff の場合 B(==A) を含まない．

log diff は，t0 < t < t1 の間に発生した write IO を集約したものだが，
hash diff は t0 と t1 の地点の snapshot を比較することで得たものなので，
(3) のケースに差が出る．

次のようなケースを考える．
時刻 t0, t1, t2 における clean snapshot をそれぞれ s0, s1, s2 とする．
s0[addr] = A だったが，t0 < t < t1 における write IO により B になり (s1[addr] = B)
t1 < t < t2 における write IO により再び A になった (s2[addr] = A)．
結果，s0 と s2 から得られた hash diff は，A を含まないが，t0 < t < t2 の間の wlog から
生成された log diff は A を含む．
この hash diff は s0 に適用すると s2 が得られるが，s1 に適用しても
B への変更を取り零してしまい，s2 にはならない．
しかし，log diff は s0 にも s1 にも適用することが出来，s2 が得られる．


## walb-tools が前提とする snapshot と diff の制約

- フルバックアップによって作られたボリュームの snapshot イメージを s0 とする．
  それに対して，差分データ diff の列 d0, d1, d2, ... がハッシュバックアップ/やログ変換により生成される．
  diff は archive server に保存完了したときに確定し，それが列になることは実装によって保証される．

- s0 に d0 を適用することができ，その結果は s1 となる．これを s0 << d0 = s1 と書く．
  同様に，s_i << d_i = s_{i+1} となる．

- diff 列の中で隣り合う diff はマージすることができる．
  それを，merged diff と呼び，d_i ++ d_{i+1} ++ ... ++ d_{j-1} = md_i_j と書く (i < j)．
  演算子 (++) は交換法則が成り立たないことに注意．
  md_i_j ++ md_j_k = md_i_k も成り立つ．
  一度マージされた diff は元に戻せない．
  {md} の集合は {d} の集合を含むものとする．md_i_{i+1} = md_i = d_i とする．

- マージされた diff は，次の条件下でのみ snapshot に適用できる．
  s_k << md_i_j = s_j  (i <= k <= j)
  ただし，md_i_j が hash-repl によって作られた s_x と s_y (x + 1 < y) の hash diff を含む場合 (*1) は，
  条件 (k = i) が必要となる．
  (s_x と s_{x+1} の hash diff については，間に snapshot を含まないため，
   差分を取り零す問題が発生しないため，hash-bkp により生成された diff は，追加条件を満たす必要はない)

- s_k << md_i_j を実行中にアーカイブイメージが中途半端な状態でプロセスが死ぬことがある．
  この中途半端な状態を applying snapshot と呼び，as_i_j と書く．
  次に再開したときは，そのイメージには以下の条件を満たす diff しか適用できない．
  as_i_j << md_i'_j' = s_j'  (i' <= i, j <= j')
  ただし，md_i_j が (*1) を満たす場合，条件は，(i' = i = k, j <= j') となる．
  この適用操作は as_i'_j' 状態を経由して，s_j' となることに注意．
  {as} の集合は {a} の集合を含むものとする．as_i = s_i とする．

- これらの制約を満たせば merge/apply 結果のデータがおかしくならないことを証明する必要はある．

-----
