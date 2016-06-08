// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
    "sync"
    "net"
    "runtime"
    "testing"
    "github.com/wangtuanjie/ip17mon"
)

// 一次最多同时处理文件个数
var wg1 sync.WaitGroup
var sema1 = make(chan struct{}, 1024)

func search(ip string, n *sync.WaitGroup) {
    sema1 <- struct{}{}        // acquire token                              
    defer func() { <-sema1 }() // release token 
    defer n.Done()
    ip17mon.Find(ip)

    if _, err := ip17mon.Find(ip); err != nil {
        return
    }


}

func TestIPResolver(t *testing.T) {
    var i int = 9
    runtime.GOMAXPROCS(24)
    if err := ip17mon.Init("data/17monipdb.dat"); err != nil {
        RecordLog("load Ipdata error")
        panic(err)
    }


    var ip string = "180.97.33.107"
    // create consumers 
    for i := 0; i < 100; i++ {
        wg1.Add(1)
        go search (ip, &wg1)
    }

    if i == 0 {
        t.Fatal("double Close() test failed: second Close() call didn't return")
    }
}

func TestParseIP(t *testing.T) {
    var ip string = "10.1.1.x"

    res := net.ParseIP(ip)
    if res != nil {
        t.Fatal("Parse 10.1.1.x failsed")
    }
}
