package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/rancher/sparse-tools/log"
	"github.com/rancher/sparse-tools/sparse"
)

func main() {
	defaultNonVerboseLogLevel := log.LevelWarn // set if -verbose is false
	// Command line parsing
	verbose := flag.Bool("verbose", false, "verbose mode")
	daemon := flag.Bool("daemon", false, "daemon mode (run on remote host)")
	port := flag.Int("port", 5000, "optional daemon port")
	timeout := flag.Int("timeout", 60, "optional daemon/client timeout (seconds)")
	host := flag.String("host", "", "remote host of <DstFile> (requires running daemon)")
	flag.Usage = func() {
		const usage = "sync <Options> <SrcFile> [<DstFile>]"
		const examples = `
Examples:
  sync -daemon
  sync -host remote.net file.data`
		fmt.Fprintf(os.Stderr, "\nUsage of %s:\n", os.Args[0])
		fmt.Fprintln(os.Stderr, usage)
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, examples)
	}
	flag.Parse()

	args := flag.Args()
	if *daemon {
		// Daemon mode
		endpoint := sparse.TCPEndPoint{"" /*bind to all*/, int16(*port)}
		if *verbose {
			fmt.Fprintln(os.Stderr, "Listening on", endpoint, "...")
		} else {
			log.LevelPush(defaultNonVerboseLogLevel)
			defer log.LevelPop()
		}

		sparse.Server(endpoint, *timeout)
	} else {
		// "local to remote"" file sync mode
		if len(args) < 1 {
			cmdError("missing file path")
		}
		srcPath := args[0]
		dstPath := srcPath
		if len(args) == 2 {
			dstPath = args[1]
		} else if len(args) > 2 {
			cmdError("too many arguments")
		}

		endpoint := sparse.TCPEndPoint{*host, int16(*port)}
		if *verbose {
			fmt.Fprintf(os.Stderr, "Syncing %s to %s@%s:%d...\n", srcPath, dstPath, endpoint.Host, endpoint.Port)
		} else {
			log.LevelPush(defaultNonVerboseLogLevel)
			defer log.LevelPop()
		}

		err := sparse.SyncFile(srcPath, endpoint, dstPath, *timeout)
		if err != nil {
			os.Exit(1)
		}
	}
}

func cmdError(msg string) {
	fmt.Fprintln(os.Stderr, "Error:", msg)
	flag.Usage()
	os.Exit(2)
}
