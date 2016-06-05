package main

import (
    "io"
    "os"
    "fmt"
    "log"
    "time"
    "sync"
    "bufio"
    "regexp"
    "strings"
    "runtime"
    "compress/gzip"
    "github.com/howeyc/fsnotify"
    "github.com/wangtuanjie/ip17mon"
)

var GOMAXPROCS int = runtime.NumCPU()

var out = make(chan string, GOMAXPROCS)
var chlist []chan string

// 一次最多同时处理文件个数
var sema = make(chan struct{}, 256)

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

var vcdn_log_format map[string]map[string]int

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


func recordLog(log string) {
	t := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println(t + " " + log + "\n")
}

func consumer(ch <-chan string, index int, out chan<- string,) {
    var ip_pattern = `((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)`
    var ip_re *regexp.Regexp = regexp.MustCompile(ip_pattern)
    var count int = 0
    key2idx := vcdn_log_format["0.3"]
    expected_fields_len := len(key2idx)

    var write_to_file string
    write_to_file = fmt.Sprintf("/mm/processed/%d", index)
    fout, _:= os.Create(write_to_file)
    defer fout.Close()

    for log := range ch {
        log = strings.TrimSpace(log)
        count ++;
        /* fout.WriteString("59.52.41.18 中国 江西 南昌 N/A\n")
        continue 

        text := line_re.FindSubmatch([]byte(log))
        if text == nil {
            fout.WriteString("OK")
            continue
        }*/

        splitted := strings.Split(log, "\" \"")

        if len(splitted) != expected_fields_len {
            continue
        }

        ip := splitted[key2idx["remote_ip"]]
        if !ip_re.MatchString(ip) {
            continue
        }

        if loc, err := ip17mon.Find(ip); err != nil {
            continue
        } else {
            fout.WriteString(fmt.Sprintf("%s %s %s %s %s %s %s\n",
                                        ip,
                                        loc.Country,
                                        loc.Region,
                                        loc.City,
                                        loc.Isp,
                                        splitted[key2idx["hstatus"]],
                                        splitted[key2idx["bbytes_sent"]],
                                    ))
        }
    }

    //recordLog(fmt.Sprintf("Thread: %d processed %d.\n", index, count))
    out <- fmt.Sprintf("%d\n", count)
}


func processLogfile(filePth string, n *sync.WaitGroup, consumerNumber int) error {

    sema <- struct{}{}        // acquire token                              
    defer func() { <-sema }() // release token 

    defer n.Done()

    f, err := os.Open(filePth)
    if err != nil {
        return err
    }
    defer f.Close()

    // gzip read
    gr, err := gzip.NewReader(f)
    if err != nil {
        panic(err)
    }
    defer gr.Close()

    // tar read
    bfRd := bufio.NewReader(gr)

    r := 0
    for {
        line, err := bfRd.ReadString('\n')
        r++
        idx := r % consumerNumber
        chlist[idx] <- line
        if err != nil && err == io.EOF {
            break
        }
    }

    fmt.Printf("Finished processing %s with %d records\n", filePth, r)

    return nil
}

func watch(dir string) {

    watcher, err := fsnotify.NewWatcher()
    if err != nil {
        log.Fatal(err)
    }

    done := make(chan bool)

    // Process events
    go func() {
        for {
            select {
            case ev := <-watcher.Event:
                log.Println("event:", ev)
                log.Println("done")
            case err := <-watcher.Error:
                log.Println("error:", err)
            }
        }
    }()

    err = watcher.WatchFlags(dir, fsnotify.FSN_CREATE)
    if err != nil {
        log.Fatal(err)
    }

    // Hang so program doesn't exit
    <-done

    /* ... do stuff ... */
    watcher.Close()
}


func main() {
    runtime.GOMAXPROCS(GOMAXPROCS)


    vcdn_log_format = make(map[string]map[string]int)
    vcdn_log_format["0.3"] = map[string]int{
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


    // TODO: need implement
    //watch("/mm")


    // create chanel
    for i := 0; i < GOMAXPROCS; i++ {
        chlist = append(chlist, make(chan string, 4096))
    }

    // create consumers 
    for i := 0; i < GOMAXPROCS; i++ {
        go consumer(chlist[i], i, out)
    }


    // load ipdb
    if err := ip17mon.Init("17monipdb.dat"); err != nil {
        recordLog("load Ipdata error")
        panic(err)
    }


    // Prepare files list
    var file_list []string;
    for i :=0; i < 1200; i++ {
        this_file := fmt.Sprintf("/mm/%d.gz", i)
        file_list = append(file_list, this_file)
    }


    // for wait
    var wg sync.WaitGroup

    for _, file := range file_list {
        fmt.Printf("Start processing %s\n", file)
        wg.Add(1)
        go processLogfile(file, &wg, GOMAXPROCS)
    }

    go func() {
        wg.Wait()
        for i := 0; i < GOMAXPROCS; i++ {
            close(chlist[i])
        }
    }()

    // show the report for each consumer
    for i := 0; i < GOMAXPROCS; i++ {
        msg := <-out
        fmt.Printf("Thread<%d> finished %s", i, msg)
    }

    recordLog("Done.")
}
