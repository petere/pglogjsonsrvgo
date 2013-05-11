// Consumer for pg_logforward JSON data

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
)

const (
	MAX_UDP_SIZE = 65535 // more or less
)

type LogEntry struct {
	Username      string
	Database      string
	Remotehost    string
	Query         string "debug_query_string"
	Elevel        int
	Funcname      string
	Sqlerrcode    int
	Message       string
	Detail        string
	Hint          string
	Context       string
	InstanceLabel string "instance_label"
}

func main() {
	if len(os.Args) != 2 || os.Args[1] == "--help" {
		fmt.Printf("Usage: %s UDPADDR\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}

	udpAddr, err := net.ResolveUDPAddr("udp", os.Args[1])
	if err != nil {
		log.Fatal("could not resolve UDP address", err)
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatal("could not set up listener", err)
	}

	defer udpConn.Close()

	var buf [MAX_UDP_SIZE]byte
	for {
		n, err := udpConn.Read(buf[:])
		if err != nil {
			log.Println("could not read", err)
		}

		var le LogEntry
		err = json.Unmarshal(buf[:n], &le)
		if err != nil {
			log.Println("could not decode JSON", err)
		}
		fmt.Println("Read: ", le)

	}
}
