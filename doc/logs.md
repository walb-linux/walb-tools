# Logs

Walb-tools uses cybozu logger provided by `cybozulib`.

There is four levels of logs, `error`, `warning`, `info`, and `debug`.

Default log level is `info` so `debug` logs will not be put.
All c++ executable binaries accept `-debug` option.
If the option is specified, `debug` logs will be put.

The default log output stream is standard error.
For server processes, specify log file with full path by `-l` command-line option,
then the logs will be put into the file instead of standard error.

-----



