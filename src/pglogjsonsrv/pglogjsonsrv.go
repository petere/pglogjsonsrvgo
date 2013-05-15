// Consumer for pg_logforward JSON data

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/bmizerany/pq"
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
	if len(os.Args) != 3 || os.Args[1] == "--help" {
		fmt.Printf("Usage: %s UDPADDR DBCONN\n", filepath.Base(os.Args[0]))
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

	db, err := sql.Open("postgres", os.Args[2])
	if err != nil {
		log.Fatal("could not connect to database", err)
	}

	defer db.Close()

	err = setupTables(db)
	if err != nil {
		log.Fatal("could not set up tables", err)
	}

	var buf [MAX_UDP_SIZE]byte
	for {
		n, err := udpConn.Read(buf[:])
		if err != nil {
			log.Println("could not read", err)
			continue
		}

		go handlePacket(buf[:n], db)
	}
}

func setupTables(db *sql.DB) error {
	rows, err := db.Query("SELECT * FROM information_schema.tables WHERE table_name = 'log_entries'")
	if err != nil {
		return err
	}

	if !rows.Next() {
		_, err := db.Exec("CREATE TABLE log_entries (elevel int, message text)")
		if err != nil {
			return err
		}
	}

	return nil
}

func handlePacket(buf []byte, db *sql.DB) {
	var le LogEntry
	err := json.Unmarshal(buf, &le)
	if err != nil {
		log.Println("could not decode JSON", err)
	}

	_, err = db.Exec("insert into log_entries (elevel, message) values ($1, $2)", le.Elevel, le.Message)
	if err != nil {
		log.Println("db exec failed", err)
	}
}
