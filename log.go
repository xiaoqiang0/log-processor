package main

import (
    "fmt"
    "time"
)

func RecordLog(log string) {
	t := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println(t + " " + log + "\n")
}
