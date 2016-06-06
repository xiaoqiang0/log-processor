package main

import (
    "regexp"
    "strings"
)

func Verbose(s string) string {
    ss := strings.FieldsFunc(s, func(r rune) bool { return r == '\r' || r == '\n' })
    for i, t := range ss {
        for j, k := 0, 0; ; j += k + 1 {
            k = strings.IndexByte(t[j:], '#')
            if k < 0 {
                break
            } else if k == 0 {
                ss[i] = t[:j]
                break
            } else if t[j+k-1] != '\\' {
                ss[i] = t[:j+k]
                break
            }
        }
    }
    return strings.Join(strings.Fields(strings.Join(ss, "")), "")
}

func vcdn_re_pattern() *regexp.Regexp{
    line_pattern := Verbose(`
        ^
        \"dispatcher\"
        \s\"0.3\"
        \s\"(?P<remote_ip>[^\"]*)\"
        \s\"(?P<host>[^\"]*)\"
        \s\"(?P<zone_name>[^\"]*)\"
        \s\"(?P<idc_name>[^\"]*)\"
        \s\"(?P<vcdn_ip>[^\"]*)\"
        \s\"(?P<remote_user>[^\"]*)\"
        \s\"(?P<time_local>[^\"]*)\"
        \s\"(?P<request>[^\"]*)\"
        \s\"(?P<hstatus>[^\"]*)\"
        \s\"(?P<body_bytes_sent>[^\"]*)\"
        \s\"(?P<retime>[^\"]*)\"
        \s\"(?P<uuid>[^\"]*)\"
        \s\"(?P<http_referer>[^\"]*)\"
        \s\"(?P<UA>[^\"]*)\"
        \s\"(?P<hreferer>[^\"]*)\"
        \s\"(?P<server_ip>[^\"]*)\"
        \s\"(?P<hotreq>[^\"]*)\"
        \s\"(?P<qiyi_id>[^\"]*)\"
        \s\"(?P<qiyi_pid>[^\"]*)\"
        \s\"(?P<tcp_rtt>[^\"]*)\"
        \s\"(?P<tcp_rttvar>[^\"]*)\"
        \s\"(?P<extends>[^\"]*)\"
        $
    `)

    return regexp.MustCompile(line_pattern)
}

