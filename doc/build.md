# Build

## Target system

- Architecture: x86_64
  - Currently 32bit x86 is not supported. It may work.
- Operation system: Linux distribution.
  - Linux kernel version: 3.X (or later). It depends on walb device drivers.
  - lvm2 is required to manage archive data.

Walb-tools must not work in another posix architecture.
Walb kernel device driver and walb-tools assume the same integer endian.
Do not use heterogeneous environments.


## Required tools and libraries

- C++11 compiler and linker.
  - g++-4.8, 4.9 and clang++ 3.4 are confirmed.
- binutils and make.
- Libraries
  - libaio
  - libsnappy
  - liblzma
  - libz
  - binutils-dev (for BFD=1)
  - libiberty-dev (for BFD=1 and STATIC=1)
```
> sudo apt-get install libaio-dev libsnappy-dev liblzma-dev zlib1g-dev
```


## Build

```
> cd walb-tools.git
> make
```

You will get executables in binsrc/ directory.

You can specify make options.

| Option name | description  |
|-------------|--------------|
| DEBUG=1     | debug build  |
| STATIC=1    | static build |

You can specify make target like `build`/`clean`/`rebuild`.

See `Makefile` for details.


## Install

Install executable binaries to a directory you like:
```
> sudo cp -a `make echo_binaries` /usr/local/bin/
```

Install python package if necessary:
```
> cd walb-tools.git/python
> sudo python setup.py install
```
You can use `walb-tools.git/python/walblib/__init__.py` directly.


## Environment

`walb-proxy` and `walb-archive` process may open many files at once,
especially when they have many wdiff files.
It is recommended to enlarge nofile limit.

-----
