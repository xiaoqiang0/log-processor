package main

import (
)

var VCDN_LOG_FORMAT map[string][]string
var VCDN_LOG_KEY2IDX map[string]int

func logformat_init() {
    VCDN_LOG_FORMAT = make(map[string][]string)
    VCDN_LOG_KEY2IDX = make(map[string]int)
    VCDN_LOG_FORMAT["fields"] = []string{
        "head",
        "version",
        "remote_ip",
        "host",
        "zone_name",
        "idc_name",
        "vcdn_ip",
        "remote_user",
        "time_local",
        "request",
        "hstatus",
        "bbytes_sent",
        "retime",
        "uuid",
        "http_referer",
        "UA",
        "hreferer",
        "server_ip",
        "hotreq",
        "qiyi_id",
        "qiyi_pid",
        "tcp_rtt",
        "tcp_rttvar",
        "extends",
    }

    for idx, field := range(VCDN_LOG_FORMAT["fields"]) {
        VCDN_LOG_KEY2IDX[field] = idx
    }

}
