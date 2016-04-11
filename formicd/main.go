package main

import (
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/pandemicsyn/cmdctrl2"
)

func main() {
	cfg := resolveConfig(nil)
	// TODO: Hardcoded CmdCtrl config for now.
	cmdCtrlServer := cmdctrl2.New(&cmdctrl2.Config{
		ListenAddress: cfg.cmdCtrlListenAddress,
		CertFile:      path.Join(cfg.path, "server.crt"),
		KeyFile:       path.Join(cfg.path, "server.key"),
	}, newService(cfg))
	doneChan := cmdCtrlServer.Start()
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-signalChan:
			cmdCtrlServer.Stop()
			return
		case <-doneChan:
			return
		}
	}
}
