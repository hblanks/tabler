package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"tabler"
)

func usage() {
	fmt.Fprintf(os.Stderr,
		"Usage: %s [options] (TABLES_JSON DSN |-generate-tables)\n\n",
		os.Args[0])
	fmt.Fprintf(os.Stderr,
		"Reads messages from stdin or TCP."+
			"Writes them to tables in a DB.\n"+
			"TABLES_JSON maps message types to table structures.\n"+
			"DSN is a DB DSN (sqllite:// only)\n"+
			"To generate TABLES_JSON from existing data, "+
			"use -generate-tables\n\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func generateTables(listenSocket string, inputFormat string) int {
	tablerInst := tabler.NewTabler()
	defer tablerInst.Close()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
		s := <-c
		fmt.Println("tabler: received signal ", s)
		tablerInst.Close()
		os.Exit(0)
	}()

	err := tablerInst.Init(listenSocket, inputFormat)
	if err != nil {
		log.Printf("tabler: init-failure error=%v", err)
		return 1
	}

	var tablesJSON []byte
	tablesJSON, err = tablerInst.GenerateTables()
	if err != nil {
		log.Printf("tabler: generate-tables error=%v", err)
		return 1
	}
	os.Stdout.Write(tablesJSON)
	return 0
}

func writeRows(listenSocket string, inputFormat string, tablesJSON string, dsn string) int {
	tablerInst := tabler.NewTabler()
	defer tablerInst.Close()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
		s := <-c
		fmt.Println("tabler: received signal ", s)
		tablerInst.Close()
		os.Exit(0)
	}()

	err := tablerInst.Init(listenSocket, inputFormat)
	if err != nil {
		log.Printf("tabler: init-failure error=%v", err)
		return 1
	}
	err = tablerInst.WriteRows(tablesJSON, dsn)
	if err != nil {
		log.Printf("tabler: write-failure error=%v", err)
		return 1
	}
	return 0
}

func main() {
	help := flag.Bool("h", false, "Print help")
	listenSocket := flag.String("l", "",
		"Bind and read from TCP socket instead of stdin")
	inputFormat := flag.String("format", "json",
		"Input format (default: json):\n"+
			"    json: newline-delimited JSON objects containing \"type\"\n"+
			"    heka: heka protobuf messages\n"+
			"    heka-stream: stream of heka protobuf messages")
	generateTablesFlag := flag.Bool("generate-tables", false,
		"use input data to generate & print a tables.json file to STDOUT")

	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")

	flag.Parse()
	if *help {
		usage()
	}

	if *generateTablesFlag {
		os.Exit(generateTables(*listenSocket, *inputFormat))
	} else {
		if flag.NArg() != 2 {
			usage()
		}

		f := func() int {
			if *cpuprofile != "" {
				f, err := os.Create(*cpuprofile)
				if err != nil {
					log.Fatal(err)
				}
				pprof.StartCPUProfile(f)
				defer pprof.StopCPUProfile()
			}
			result := writeRows(*listenSocket, *inputFormat, flag.Arg(0), flag.Arg(1))
			return result
		}
		os.Exit(f())
	}
}
