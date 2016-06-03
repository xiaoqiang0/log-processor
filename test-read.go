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
    "strconv"
    "strings"
)

func recordLog(log string) {
	t := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println(t + " " + log + "\n")
}


func consumer(ch <-chan string, index int) {
    var ip_pattern = `((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)`
    var reg *regexp.Regexp
    reg = regexp.MustCompile(ip_pattern)
    var count int = 0
    for log := range ch {
        splitted := strings.Split(log, "\" \"")
        count ++;
        ip := splitted[2]
        if len(splitted) == 1 {
            continue
        }
        if !reg.MatchString(ip) {
            continue
        }
        if loc, err := ip17mon.Find(ip); err != nil {
            fmt.Fprintf(os.Stderr, "%s: %v\n", os.Args[1], err)
            os.Exit(1)
        } else {
            fmt.Println(loc.Country, loc.Region, loc.City, loc.Isp)
        }
        //fmt.Println(ip)
        /* pos1 := str.IndexOf(log, "GET /t.gif?", 0)
        if pos1 == -1 {
            continue
        } */
        //fmt.Println("1")
    }

    fmt.Println("Thread: ", index, "processed: ", count)
}


func ReadLine(filePth string, consumerNumber int) error {
    f, err := os.Open(filePth)
    if err != nil {
        return err
    }

    defer f.Close()

    // Go
    var ch = make(chan string, consumerNumber)
    for i := 0; i < consumerNumber; i++ {
        go consumer(ch, i)
    }

    r := 0
    bfRd := bufio.NewReader(f)
    for {
        line, err := bfRd.ReadString('\n')
        ch <- line

        r++
        if r%10000 == 0 {
            recordLog("readline:" + strconv.Itoa(r))
        }
        if err != nil {
            if err == io.EOF {
                break
                recordLog("wait, readline:" + strconv.Itoa(r))
            } else {
                recordLog("ReadString err")
            }
        }
    }
    return nil
}

func main() {
    runtime.GOMAXPROCS(20)
    if err := ip17mon.Init("17monipdb.dat"); err != nil {
        recordLog("load Ipdata error")
        panic(err)
    }
    var file string = "access.log"
    ReadLine(file, 20)
    recordLog("File Read Done. redisPool")
}
