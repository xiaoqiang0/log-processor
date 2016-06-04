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


func consumer(ch <-chan string, index int, out chan<- string,) {
    var ip_pattern = `((2[0-4]\d|25[0-5]|[01]?\d\d?)\.){3}(2[0-4]\d|25[0-5]|[01]?\d\d?)`
    var reg *regexp.Regexp
    reg = regexp.MustCompile(ip_pattern)
    var count int = 0
    var processed_file string
    processed_file = fmt.Sprintf("/mm/processed_%d", index)
    fout, _:= os.Create(processed_file)
    defer fout.Close()
    for log := range ch {
        //fmt.Println("OK")
        if log == "done" {
            break
        }
        //fout.WriteString("182.87.232.13 中国 江西 上饶 N/A\n")
        //continue
        splitted := strings.Split(log, "\" \"")
        count ++;
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
    recordLog(fmt.Sprintf("Thread: %d processed %d.\n", index, count))
    out <- fmt.Sprintf("%d\n", count)
}


func ReadLine(filePth string, consumerNumber int) error {
    f, err := os.Open(filePth)
    if err != nil {
        return err
    }

    defer f.Close()

    // Go
    //var ch = make(chan string, consumerNumber)
    var chlist []chan string
    for i := 0; i < consumerNumber; i++ {
        chlist = append(chlist, make(chan string, 4096))
    }
    var out = make(chan string, consumerNumber)
    for i := 0; i < consumerNumber; i++ {
        go consumer(chlist[i], i, out)
    }

    r := 0
    bfRd := bufio.NewReader(f)
    for {
        line, err := bfRd.ReadString('\n')
        r++
        idx := r % consumerNumber
        chlist[idx] <- line

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
    for i := 0; i < consumerNumber; i++ {
        //ch <- "done"
        chlist[i] <- "done"
    }

    for i := 0; i < consumerNumber; i++ {
        msg := <-out
        fmt.Println("DONE--------------", msg)
    }
    return nil
}

func main() {
    runtime.GOMAXPROCS(24)
    if err := ip17mon.Init("17monipdb.dat"); err != nil {
        recordLog("load Ipdata error")
        panic(err)
    }
    var file string = "/mm/access.log.big"
    ReadLine(file, 24)
    recordLog("File Read Done. redisPool")
}
