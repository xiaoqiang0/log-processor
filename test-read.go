package main

import (
    "bufio"
    "time"
    "io"
    "fmt"
    str "github.com/mgutz/str"
    "os"
    "strconv"
    "strings"
)

func recordLog(log string) {
	t := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println(t + " " + log + "\n")
}


func consumer(ch <-chan string, index int) {
    for log := range ch {
        splitted := strings.Split(log, "\" \"")
        if len(splitted) == 1 {
            continue
        }
        pos1 := str.IndexOf(log, "GET /t.gif?", 0)
        if pos1 == -1 {
            continue
        }
        fmt.Println("1")
    }
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
    var file string = "access.log"
    ReadLine(file, 20)
    recordLog("File Read Done. redisPool")
}
