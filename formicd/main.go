package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	pb "github.com/creiht/formic/proto"
	"github.com/pandemicsyn/ftls"
	"github.com/pandemicsyn/oort/api"

	"net"
)

// FatalIf is just a lazy log/panic on error func
func FatalIf(err error, msg string) {
	if err != nil {
		grpclog.Fatalf("%s: %v", msg, err)
	}
}

var (
	printVersionInfo = flag.Bool("version", false, "print version/build info")
)

var formicdVersion string
var buildDate string
var goVersion string

func main() {
	flag.Parse()
	if *printVersionInfo {
		fmt.Println("formicd version:", formicdVersion)
		fmt.Println("build date:", buildDate)
		fmt.Println("go version:", goVersion)
		return
	}

	cfg := resolveConfig(nil)

	var opts []grpc.ServerOption
	creds, err := credentials.NewServerTLSFromFile(path.Join(cfg.path, "server.crt"), path.Join(cfg.path, "server.key"))
	FatalIf(err, "Couldn't load cert from file")
	opts = []grpc.ServerOption{grpc.Creds(creds)}
	s := grpc.NewServer(opts...)

	var cOpts []grpc.DialOption
	tlsConfig := &ftls.Config{
		MutualTLS:          !cfg.skipMutualTLS,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		CertFile:           path.Join(cfg.path, "client.crt"),
		KeyFile:            path.Join(cfg.path, "client.key"),
		CAFile:             path.Join(cfg.path, "ca.pem"),
	}
	rOpts, err := ftls.NewGRPCClientDialOpt(&ftls.Config{
		MutualTLS:          false,
		InsecureSkipVerify: cfg.insecureSkipVerify,
		CAFile:             path.Join(cfg.path, "ca.pem"),
	})
	if err != nil {
		grpclog.Fatalln("Cannot setup tls config for synd client:", err)
	}

	clientID, _ := os.Hostname()
	if clientID != "" {
		clientID += "/formicd"
	}

	vstore := api.NewReplValueStore(&api.ReplValueStoreConfig{
		AddressIndex:       2,
		StoreFTLSConfig:    tlsConfig,
		GRPCOpts:           cOpts,
		RingServer:         cfg.oortValueSyndicate,
		RingCachePath:      path.Join(cfg.path, "ring/valuestore.ring"),
		RingServerGRPCOpts: []grpc.DialOption{rOpts},
		RingClientID:       clientID,
	})
	if verr := vstore.Startup(context.Background()); verr != nil {
		grpclog.Fatalln("Cannot start valuestore connector:", verr)
	}

	gstore := api.NewReplGroupStore(&api.ReplGroupStoreConfig{
		AddressIndex:       2,
		StoreFTLSConfig:    tlsConfig,
		GRPCOpts:           cOpts,
		RingServer:         cfg.oortGroupSyndicate,
		RingCachePath:      path.Join(cfg.path, "ring/groupstore.ring"),
		RingServerGRPCOpts: []grpc.DialOption{rOpts},
		RingClientID:       clientID,
	})
	if gerr := gstore.Startup(context.Background()); gerr != nil {
		grpclog.Fatalln("Cannot start valuestore connector:", gerr)
	}

	fs, err := NewOortFS(vstore, gstore)
	if err != nil {
		grpclog.Fatalln(err)
	}
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.port))
	FatalIf(err, "Failed to bind to port")
	pb.RegisterApiServer(s, NewApiServer(fs, cfg.nodeId))
	grpclog.Printf("Starting up on %d...\n", cfg.port)
	s.Serve(l)
}
