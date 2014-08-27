# Specification

This documents indicates appropriate source code.

## Walb device controller

See help messages:
```
> binsrc/wdevc -h
```

see help for each command:
```
> binsrc/wdevc COMMAND_NAME -h
```

## Walb-tools daemon controller

```
> binsrc/walbc -h
```

Command lists:
- src/storage.hpp: see storageHandlerMap and storageGetHandlerMap.
- src/proxy.hpp: see proxyHandlerMap and proxyGetHandlerMap.
- src/archive.hpp: see archiveHandlerMap and archiveGetHandlerMap.

**TODO**: make option parsing and help of walbc better.

## Protocols

Object serializer.
- src/packet.hpp
- cybozulib/include/serializer.hpp

Protocol messages and stuffs.
- src/protocol.hpp

## Server processes

All servers are single-process, multi-thread.
You can specify log output as stderr or a reguar file.
Debug option `-debug` will puts debug logs.

These executables do not daemonize themselves.
You can use daemonization tools like upstart.

## Storage server

Command line
```
> binsrc/walb-storage -h
```

State transition.

- src/storage_constants.hpp

Other related code.

- binsrc/walb-storage.cpp
- src/storage.hpp
- src/storage_vol_info.hpp

## Proxy server

Command line
```
> binsrc/walb-proxy -h
```

State transition.

- src/proxy_constants.hpp

Other related code.

- binsrc/walb-proxy.cpp
- src/proxy.hpp
- src/proxy_vol_info.hpp

## Archive server

Command line
```
> binsrc/walb-archive -h
```

State transition.

- src/archive_constants.hpp

Other related code.

- binsrc/walb-archive.cpp
- src/archive.hpp
- src/archive_vol_info.hpp

-----
