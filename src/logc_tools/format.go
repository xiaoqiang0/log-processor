package logc_tools

import ()

var VCDN_LOG_FORMAT map[string][]string
var VCDN_LOG_KEY2IDX map[string]int

func Logformat_init() {
	VCDN_LOG_FORMAT = make(map[string][]string)
	VCDN_LOG_KEY2IDX = make(map[string]int)
	VCDN_LOG_FORMAT["fields"] = []string{
		"head",
		"version",
		"hitstatus",
		"remote_ip",
		"server_ip",
		"datetime",
		"uri",
		"args",
		"hprotocol",
		"hstatus",
		"bbytes_sent",
		"bytes_sent",
		"retime",
		"urtime",
		"f4v2ts_time",
		"mp4tots_time",
		"upstream_addr",
		"hreferer",
		"UA",
		"rxfip",
		"hrange",
		"qiyi_apptag",
		"tcp_rtt",
		"tcp_rttvar",
		"ups_h_m",
		"ups_p_hit",
		"ups_p_miss",
		"qiyi_id",
		"qiyi_pid",
		"hit_type",
		"ts_hit_type",
		"client_type",
		"extends",
	}

	for idx, field := range VCDN_LOG_FORMAT["fields"] {
		VCDN_LOG_KEY2IDX[field] = idx
	}

}
