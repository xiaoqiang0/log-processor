package logc_config

import (
	"log"
	"os"
)

const (
	IP_17MON_DAT = "../data/17monipdb.dat"
	RESOURCE_DIR = "/mm"
	CONSUMER_DIR = "/root"
	RESULT_DIR   = "/root"
)

func isDirExists(path string) {
	log.Printf("[Check dir:%v ]\n", path)
	fi, err := os.Stat(path)

	if err != nil {
		log.Fatalf("[Error Not exist (dir:%v)]\n", path)
	}
	if fi.IsDir() {
		log.Printf("[Check done dir:%v ]\n", path)
	} else {
		log.Fatalf("[Exit (%v is a file)]\n", path)
	}
}

func isFileExist(filename string) {
	log.Printf("[Check file:%v ]\n", filename)
	fi, err := os.Stat(filename)

	if err != nil {
		log.Fatalf("[Error Not exist (file:%v)]\n", filename)
	}
	if fi.IsDir() {
		log.Fatalf("[Exit (%v is a dir)]\n", filename)
	} else {
		log.Printf("[Check done file:%v ]\n", filename)
	}

}

func CheckConfigDir() {
	//isFileExist(IP_17MON_DAT)
	isDirExists(RESULT_DIR)
	isDirExists(CONSUMER_DIR)
	isDirExists(RESOURCE_DIR)
}
