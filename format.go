package main

import (
)

var VCDN_LOG_FORMAT map[string]map[string]int

func logformat_init() {
    VCDN_LOG_FORMAT = make(map[string]map[string]int)
    VCDN_LOG_FORMAT["0.3"] = map[string]int{
        "head": 0,
        "version": 1,
        "remote_ip": 2,
        "host": 3,
        "zone_name": 4,
        "idc_name": 5,
        "vcdn_ip": 6,
        "remote_user": 7,
        "time_local": 8,
        "request": 9,
        "hstatus": 10,
        "bbytes_sent": 11,
        "retime": 12,
        "uuid": 13,
        "http_referer": 14,
        "UA": 15,
        "hreferer": 16,
        "server_ip": 17,
        "hotreq": 18,
        "qiyi_id": 19,
        "qiyi_pid": 20,
        "tcp_rtt": 21,
        "tcp_rttvar": 22,
        "extends": 23,
    }
}
