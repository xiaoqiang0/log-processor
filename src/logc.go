package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"github.com/howeyc/fsnotify"
	"io"
	"log"
	"logc_config"
	"logc_tools"
	"os"
	"qiyi_ip17mon"
	"regexp"
	"runtime"
	"sync"
)

const (
	MAXPARALLEL = 256
	QUOTE       = 34
	BREAK_LINE  = 10
	SPACE       = 32
	DOT         = 46
)

var GOMAXPROCS int = runtime.NumCPU()

var chlist []chan []byte

// 一次最多同时处理文件个数
var sema = make(chan struct{}, MAXPARALLEL)
var out = make(chan string, GOMAXPROCS)

// 等待所有文件处理结束
var wg sync.WaitGroup

var fieldSep []byte
var breakLineSep []byte
var space []byte
var dot []byte

func sepInit() {
	fieldSep = []byte{QUOTE, SPACE, QUOTE}
	breakLineSep = []byte{BREAK_LINE}
	space = []byte{SPACE}
	dot = []byte{DOT}
}

//parse ip and  converts the IPv4 address ip to a 4-byte representation.
func parsedIp(ip []byte) []byte {
	var ip4 []byte
	ip_arr := bytes.Split(ip, dot)
	if len(ip_arr) != 4 {
		return nil
	}
	for _, p := range ip_arr {
		tmp := bytes2byte(p)
		if tmp == 32 {
			return nil
		}
		ip4 = append(ip4, tmp)
	}
	return ip4
}

func bytes2byte(bytes []byte) byte {
	var bt byte
	bt = 0
	for _, b := range bytes {
		if (b < 48) || (b > 57) {
			return 32
		}
		bt = bt<<3 + bt<<1 + b - 48
	}
	if bt > 255 {
		return 32
	}
	return bt
}

func processLine(log []byte) []byte {
	var buffer bytes.Buffer

	seq_num := 0
	start := 0
	n := bytes.Count(log, fieldSep) + 1
	if n != len(logc_tools.VCDN_LOG_FORMAT["fields"]) {
		return nil
	}
	splitted := make([][]byte, n)
	for i := 0; i < len(log)-3 && seq_num < n-1; i++ {
		if log[i] == QUOTE && log[i+1] == SPACE && log[i+2] == QUOTE {
			splitted[seq_num] = log[start:i]
			seq_num++
			start = i + 3
			i = i + 2
		}
	}

	ip := splitted[logc_tools.VCDN_LOG_KEY2IDX["remote_ip"]]
	parsed_ip := parsedIp(ip)
	if parsed_ip == nil {
		return nil
	}
	loc := qiyi_ip17mon.FindByteByUint(binary.BigEndian.Uint32(parsed_ip))

	buffer.Write(ip)
	buffer.Write(space)
	buffer.Write(loc.Country)
	buffer.Write(space)
	buffer.Write(loc.Region)
	buffer.Write(space)
	buffer.Write(loc.City)
	buffer.Write(space)
	buffer.Write(loc.Isp)
	buffer.Write(space)
	buffer.Write(splitted[logc_tools.VCDN_LOG_KEY2IDX["hstatus"]])
	buffer.Write(space)
	buffer.Write(splitted[logc_tools.VCDN_LOG_KEY2IDX["bbytes_sent"]])
	buffer.Write(space)
	buffer.Write(breakLineSep)
	return buffer.Bytes()
}

func consumer(ch <-chan []byte, index int, out chan<- string) {
	var count int = 0
	var buffer bytes.Buffer

	var write_to_file string
	write_to_file = fmt.Sprintf("/mm/processed/%d", index)
	fout, _ := os.OpenFile(write_to_file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	defer fout.Close()

	for log := range ch {
		buffer.Write(processLine(log))

		if buffer.Len() > 4096 {
			fout.Write(buffer.Bytes())
			buffer.Reset()
		}
		count++
	}
	fout.Write(buffer.Bytes())
	out <- fmt.Sprintf("%d\n", count)
}

func processLogfile(filePth string, n *sync.WaitGroup, consumerNumber int) error {

	// 保证最多同时有 MAXPARALLEL 个文件并行处理，控制并发
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
	scanner := bufio.NewScanner(gr)
	c := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		//idx := r % consumerNumber
		chlist[consumerNumber] <- line
		if err != nil && err == io.EOF {
			break
		}
		c++
	}

	fmt.Printf("Finished processing %s with %d records\n", filePth, c)

	return nil
}

func watchLogDir(dir string) {

	var file_pattern = `\.gz$`
	var file_re *regexp.Regexp = regexp.MustCompile(file_pattern)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	var ch = make(chan string)

	// Setting max files to be processed
	wg.Add(48)
	i := 0
	// Process events
	go func() {
		for {
			select {
			case ev := <-watcher.Event:
				if ev != nil && file_re.MatchString(ev.Name) {
					// wg.Add(1) // wait forever ...
					watched_file := ev.Name
					go processLogfile(watched_file, &wg, i%GOMAXPROCS)
					i++
					fmt.Printf("Start processing %s\n", watched_file)
				}
			case err := <-watcher.Error:
				if err != nil {
					log.Println("error:", err)
				}
			}
		}
	}()

	if err != nil {
		log.Fatal(err)
	}

	err = watcher.WatchFlags(dir, fsnotify.FSN_CREATE)

	go func() {
		wg.Wait()
		fmt.Printf("Start close channels ...\n")
		for i := 0; i < GOMAXPROCS; i++ {
			close(chlist[i])
		}

		watcher.Close()
		fmt.Printf("Closed ...\n")
		ch <- "done\n"
		<-ch
		fmt.Printf("Sent close signal ...\n")
	}()
}

func main() {
	//logc_config.CheckConfigDir()

	runtime.GOMAXPROCS(GOMAXPROCS)

	sepInit()
	logc_tools.Logformat_init()
	// create chanel
	for i := 0; i < GOMAXPROCS; i++ {
		chlist = append(chlist, make(chan []byte, 4096))
	}

	// create consumers
	for i := 0; i < GOMAXPROCS; i++ {
		go consumer(chlist[i], i, out)
	}

	// load ipdb
	if err := qiyi_ip17mon.Init(logc_config.IP_17MON_DAT); err != nil {
		logc_tools.RecordLog("load Ipdata error")
		panic(err)
	}

	watchLogDir(logc_config.RESOURCE_DIR)

	// show the report for each consumer
	for i := 0; i < GOMAXPROCS; i++ {
		msg := <-out
		fmt.Printf("Thread<%d> finished %s", i, msg)
	}

	logc_tools.RecordLog("Done.")
}
