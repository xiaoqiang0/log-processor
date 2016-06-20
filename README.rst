Test Cases Description
======================

- Totally, 9600w records = 4800consumer*2
- rsync 4800 gz log files with each file conains 10k records cost 48s,
  100 files per seconds.
- logc cost:

::

    real    1m13.015s
    user    24m43.516s
    sys     0m15.048s


:QPS: 9600,0000/73 = 131w/s

Steps
=====

编译

::
    $ source activate
    $ cd src && go build logc.go


准备测试日志压缩文件:

::

    $ ./prepare_gz.sh

启动预处理程序:

::

    $ cd src && time ./logc >log 2>&1 &
    $ tail -f log

启动模拟rsync任务:

::

    $ cd tools && ./rsync_gz.sh

监控处理任务的 ``log`` 文件.

Optimizations
=============

tmpfs
-----


::

    $  mount -t tmpfs -o size=10G tmpfs /mm1/
    $  mount -t tmpfs -o size=10G tmpfs /mm/

max files can be watched at once
--------------------------------

    
::

    $ cat /proc/sys/fs/inotify/max_user_watches
    8192

TODO
====

- 参数支持

  * 批量模式: 指定处理多少文件后停止
  * inotify模式: 一直监控
  * 同时处理文件的个数
  * GOMAXPROCS 设置，目前默认是CPU个数


