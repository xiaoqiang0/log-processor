Test Cases Description
======================

- Totally, 4800w records
- rsync 4800 gz log files with each file conains 10k records cost 48s,
  100 files per seconds.
- log-processor cost:

::

    real    1m50.584s
    user    35m55.256s
    sys     1m40.487s


:QPS: 4800,0000/110 = 43w/s

Steps
=====

准备测试日志压缩文件:

::

    $ ./prepare_gz.sh

启动预处理程序:

::

    $ time make run >log 2>&1 &
    $ tail -f log

启动模拟rsync任务:

::

    $ ./rsync_gz.sh

监控处理任务的 ``log`` 文件.

