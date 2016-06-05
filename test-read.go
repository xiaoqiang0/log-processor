package main

import (
    "runtime"
    "bufio"
    "time"
    "io"
    "fmt"
    "regexp"
    "github.com/wangtuanjie/ip17mon"
    "os"
    "strings"
    "sync"
    "github.com/howeyc/fsnotify"
    "log"
    "compress/gzip"
)

const parallel int = 24

var out = make(chan string, parallel)
var chlist []chan string

// 一次最多同时处理文件个数
var sema = make(chan struct{}, 256)

func recordLog(log string) {
	t := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println(t + " " + log + "\n")
}

func consumer(ch <-chan string, index int, out chan<- string,) {
    var ip_pattern = `((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)`
    var line_pattern = `^\"dispatcher\"\s\"0.3\"\s\"(?P<remote_ip>[^\"]*)\"\s\"(?P<host>[^\"]*)\"\s\"(?P<zone_name>[^\"]*)\"\s\"(?P<idc_name>[^\"]*)\"\s\"(?P<vcdn_ip>[^\"]*)\"\s\"(?P<remote_user>[^\"]*)\"\s\"(?P<time_local>[^\"]*)\"\s\"(?P<request>[^\"]*)\"\s\"(?P<hstatus>[^\"]*)\"\s\"(?P<body_bytes_sent>[^\"]*)\"\s\"(?P<retime>[^\"]*)\"\s\"(?P<uuid>[^\"]*)\"\s\"(?P<http_referer>[^\"]*)\"\s\"(?P<UA>[^\"]*)\"\s\"(?P<hreferer>[^\"]*)\"\s\"(?P<server_ip>[^\"]*)\"\s\"(?P<hotreq>[^\"]*)\"\s\"(?P<qiyi_id>[^\"]*)\"\s\"(?P<qiyi_pid>[^\"]*)\"\s\"(?P<tcp_rtt>[^\"]*)\"\s\"(?P<tcp_rttvar>[^\"]*)\"\s\"(?P<extends>[^\"]*)\"$`
    var ip_re *regexp.Regexp
    var line_re *regexp.Regexp

    ip_re = regexp.MustCompile(ip_pattern)
    line_re  = regexp.MustCompile(line_pattern)

    var count int = 0

    var write_to_file string
    write_to_file = fmt.Sprintf("/mm/processed/%d", index)
    fout, _:= os.Create(write_to_file)
    defer fout.Close()

    for log := range ch {
        log = strings.TrimSpace(log)
        count ++;
        // fout.WriteString("59.52.41.18 中国 江西 南昌 N/A\n")
        // continue
        text := line_re.FindSubmatch([]byte(log))
        if text == nil {
            fout.WriteString(log)
            continue
        }

        splitted := strings.Split(log, "\" \"")

        if len(splitted) < 10 {
            continue
        }


        ip := splitted[2]
        if !ip_re.MatchString(ip) {
            continue
        }

        if loc, err := ip17mon.Find(ip); err != nil {
            continue
        } else {
            fout.WriteString(fmt.Sprintf("%s %s %s %s %s\n", ip, loc.Country, loc.Region, loc.City, loc.Isp))
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
    runtime.GOMAXPROCS(parallel)

    // TODO: need implement
    //watch("/mm")
    for i := 0; i < parallel; i++ {
        chlist = append(chlist, make(chan string, 4096))
    }

    for i := 0; i < parallel; i++ {
        go consumer(chlist[i], i, out)
    }


    if err := ip17mon.Init("17monipdb.dat"); err != nil {
        recordLog("load Ipdata error")
        panic(err)
    }

    var file_list []string;

    // Prepare files list

    for i :=0; i < 1200; i++ {
        this_file := fmt.Sprintf("/mm/%d.gz", i)
        file_list = append(file_list, this_file)
    }

    var wg sync.WaitGroup

    for _, file := range file_list {
        fmt.Printf("Start processing %s\n", file)
        wg.Add(1)
        go processLogfile(file, &wg, parallel)
    }

    go func() {
        wg.Wait()
        for i := 0; i < parallel; i++ {
            close(chlist[i])
        }
    }()

    for i := 0; i < parallel; i++ {
        msg := <-out
        fmt.Printf("Thread<%d> finished %s", i, msg)
    }

    recordLog("Done.")
}
