package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"github.com/howeyc/fsnotify"
	"github.com/wangtuanjie/ip17mon"
	"io"
	"log"
	"logc_config"
	"logc_tools"
	"net"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

var GOMAXPROCS int = runtime.NumCPU()

var MAXPARALLEL int = 256

var out = make(chan string, GOMAXPROCS)
var chlist []chan string

// 一次最多同时处理文件个数
var sema = make(chan struct{}, MAXPARALLEL)

// 等待所有文件处理结束
var wg sync.WaitGroup

func consumer(ch <-chan string, index int, out chan<- string) {
	var count int = 0
	expected_fields_len := len(logc_tools.VCDN_LOG_FORMAT["fields"])

	var write_to_file string
	write_to_file = fmt.Sprintf("/mm/processed/%d", index)
	fout, _ := os.Create(write_to_file)
	defer fout.Close()

	for log := range ch {
		log = strings.TrimSpace(log)
		count++

		splitted := strings.Split(log, "\" \"")

		if len(splitted) != expected_fields_len {
			//fmt.Printf("Failed to match valid log revored")
			continue
		}

		ip := splitted[logc_tools.VCDN_LOG_KEY2IDX["remote_ip"]]
		parsed_ip := net.ParseIP(ip)
		if parsed_ip == nil {
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
				splitted[logc_tools.VCDN_LOG_KEY2IDX["hstatus"]],
				splitted[logc_tools.VCDN_LOG_KEY2IDX["bbytes_sent"]],
			))
		}
	}

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
		line := scanner.Text()
		c++
		//idx := r % consumerNumber
		chlist[consumerNumber] <- line
		if err != nil && err == io.EOF {
			break
		}
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
	wg.Add(4800)
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
				log.Println("error:", err)
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

	logc_tools.Logformat_init()
	// create chanel
	for i := 0; i < GOMAXPROCS; i++ {
		chlist = append(chlist, make(chan string, 4096))
	}

	// create consumers
	for i := 0; i < GOMAXPROCS; i++ {
		go consumer(chlist[i], i, out)
	}

	// load ipdb
	if err := ip17mon.Init(logc_config.IP_17MON_DAT); err != nil {
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
