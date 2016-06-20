package qiyi_ip17mon

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"
)

const data = "17monipdb.dat"

func TestFind(t *testing.T) {
	if err := Init(data); err != nil {
		t.Fatal("Init failed:", err)
	}
	info, err := Find(int2ip(1944579452))

	if err != nil {
		t.Fatal("Find failed:", err)
	}

	if info.Country != "中国" {
		t.Fatal("country expect = 中国, but actual =", info.Country)
	}

	if info.Region != "浙江" {
		t.Fatal("region expect = 浙江, but actual =", info.Region)
	}

	if info.City != "嘉兴" {
		t.Fatal("city expect = 嘉兴, but actual =", info.City)
	}

	if info.Isp != Null {
		t.Fatal("isp expect = Null, but actual =", info.Isp)
	}
}

/*
func TestFindByte(t * testing.T) {
	if err := Init(data); err != nil {
		t.Fatal("Init failed:", err)
	}
	info := FindByteByUint()

}
*/
//-----------------------------------------------------------------------------
func Getips(n int) []string {
	var ips []string
	for i := 0; i < n; i++ {
		ips = append(ips, int2ip(rand.Uint32()))
	}
	return ips
}

func int2ip(nn uint32) string {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, nn)
	return ip.String()
}

// Benchmark command
//	go test -bench=Find
// Benchmark result
//	BenchmarkFind 2000000       830 ns/op
// about 120w/s
func BenchmarkFind(b *testing.B) {
	b.StopTimer()
	if err := Init(data); err != nil {
		b.Fatal("Init failed:", err)
	}
	rand.Seed(time.Now().UnixNano())
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if FindByUint(rand.Uint32()) == nil {
			b.Fatal("FindByUint found nil val")
		}
	}
}

// Benchmark command
//	go test -bench=FindStr
// Benchmark result
//	BenchmarkFind 1000000       1024 ns/op
// about 55.6w/s

func BenchmarkFindStr(b *testing.B) {
	//b.StopTimer()
	ips := Getips(b.N)
	if err := Init(data); err != nil {
		b.Fatal("Init failed:", err)
	}
	b.ResetTimer()
	fmt.Println(time.Now().UnixNano())
	for _, ip := range ips {
		_, err := Find(ip)
		if err != nil {
			b.Fatal("Find found nil val")
		}
	}
	//fmt.Println(time.Now().UnixNano())
}
