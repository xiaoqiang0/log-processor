package main

import (
    "log"
    "os"
    //"io"
    "testing"
)

func TestLog(t *testing.T) {

    //var output io.Writer = os.Stdout

    //logger := log.New(output, "", log.LstdFlags|log.Lshortfile)
    log.SetOutput(os.Stdout)
    log.SetFlags(log.LstdFlags|log.Lshortfile)
    log.Printf("hello %s\n", "world")
    // logger.Panic("Panic, exiting ...")
    // logger.Fatal("failed, exiting ...")
}
