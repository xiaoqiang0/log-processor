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
    var reg *regexp.Regexp
    reg = regexp.MustCompile(ip_pattern)
    var count int = 0
    var processed_file string
    processed_file = fmt.Sprintf("/mm/processed/%d", index)
    fout, _:= os.Create(processed_file)
    defer fout.Close()

    for log := range ch {
        splitted := strings.Split(log, "\" \"")
        count ++;
        // fout.WriteString("59.52.41.18 中国 江西 南昌 N/A\n")
        // continue
        if len(splitted) < 10 {
            continue
        }

        ip := splitted[2]
        if !reg.MatchString(ip) {
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

    // Go
    //var ch = make(chan string, consumerNumber)
    r := 0
    bfRd := bufio.NewReader(f)
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
        this_file := fmt.Sprintf("/mm/%d", i)
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
