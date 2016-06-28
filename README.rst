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

LOG CONTENT
========

vcdn
----

1.原始日志各字段:

===  ==============  ======================================
seq  field           info
===  ==============  ======================================
01   "vcdn"          日志类型
02   "0.4"           版本
03   hitstatus       eg: "HIT", "MISS"
04   remote_ip        
05   server_ip
06   datetime        14位数字 eg: 20160531140000
07   uri             (用于计算channel, user, v_type)
08   args            (用于计算isp)
09   hprotocol       eg: "HTTP/1.1"
10   hstatus         http状态码 eg: "200"
11   bbytes_sent     
12   bytes_sent      (用于tranfic 计算)
13   retime          响应时间, 会有 0.0 的情况
14   urtime
15   f4v2ts_time
16   mp4tots_time
17   upstream_addr   eg: "127.0.0.1:81"
18   hreferer
19   UA              user agent(用于计算p2p)
20   rxfip           (用于计算p2p)
21   hrange
22   qiyi_apptag
23   tcp_rtt
24   tcp_rttvar
25   ups_h_m
26   ups_p_hit
27   ups_p_miss
28   qiyi_id
29   qiyi_pid
30   hit_type
31   ts_hit_type
32   client_type
33   extends
===  ==============  ======================================

2.输出结果字段

===  ==============  ====================================================
seq  field           info
===  ==============  ====================================================
01   p2p             根据UA, rxfip 计算
02   request         常量 1
03   vidc            根据server_ip 计算
04   isp             根据remote_ip, args 计算
05   prov            根据remote_ip, args 计算
06   zone            根据remote_ip, args 计算
07   traffic         根据hitstatus, tranfic计算
08   uri
09   hstatus
10   hitstatus
11   retime
12   timestamp       时间戳(取自文件名中的时间后计算) eg: 1467108000
13   minute          分钟时间(取自文件名中的时间后计算) eg: 201606281800
14   user            根据uri 计算
15   channel         根据uri 计算
16   v_type          根据uri 计算
===  ==============  ====================================================

TODO
====

- 参数支持

  * 批量模式: 指定处理多少文件后停止
  * inotify模式: 一直监控
  * 同时处理文件的个数
  * GOMAXPROCS 设置，目前默认是CPU个数


