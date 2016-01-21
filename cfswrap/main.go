// cfswrap is a wrapper for the linux mount command
//   to run the cfs fuse client
//
//

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"
)

// USER and GROUP ID for the root user
const (
	UID  = 0
	GUID = 0
)

func main() {

	var clargs []string
	// Parsing command line arguments
	flag.Parse()

	// Grab the current PATH
	path, err := exec.LookPath("cfs")
	if err != nil {
		log.Fatalf("failed to find the cfs program: %v", err)
		os.Exit(1)
	}

	// Working with command line arguments to pass them thru to cfs
	clargs = append([]string{path}, flag.Args()...)

	// The Credential fields are used to set UID, GID and attitional GIDS of the process
	// You need to run the program as  root to do this
	var cred = &syscall.Credential{UID, GUID, []uint32{}}
	// the Noctty flag is used to detach the process from parent tty
	var sysproc = &syscall.SysProcAttr{Credential: cred, Noctty: true}
	var attr = os.ProcAttr{
		Dir: ".",
		Env: os.Environ(),
		Files: []*os.File{
			os.Stdin,
			os.Stdout,
			os.Stderr,
		},
		Sys: sysproc,
	}
	process, err := os.StartProcess(path, clargs, &attr)
	if err == nil {
		fmt.Println(process.Pid)
		// It is not clear from docs, but Realease actually detaches the process
		err = process.Release()
		if err != nil {
			fmt.Println(err.Error())
		}

	} else {
		fmt.Println(err.Error())
	}
}
