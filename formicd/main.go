package main

import (
	"flag"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	pb "github.com/creiht/formic/proto"

	"net"
)

var (
	tls      = flag.Bool("tls", true, "Connection uses TLS if true, else plain TCP")
	certFile = flag.String("cert_file", "/etc/oort/server.crt", "The TLS cert file")
	keyFile  = flag.String("key_file", "/etc/oort/server.key", "The TLS key file")
	port     = flag.Int("port", 8443, "The server port")
	oortHost = flag.String("oorthost", "127.0.0.1:6379", "host:port to use when connecting to oort")
)

// FatalIf is just a lazy log/panic on error func
func FatalIf(err error, msg string) {
	if err != nil {
		grpclog.Fatalf("%s: %v", msg, err)
	}
}

func main() {
	flag.Parse()

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	FatalIf(err, "Failed to bind to port")

	var opts []grpc.ServerOption
	if *tls {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		FatalIf(err, "Couldn't load cert from file")
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	s := grpc.NewServer(opts...)
	pb.RegisterApiServer(s, NewApiServer(NewInMemDS(), NewOortFS(*oortHost)))
	grpclog.Printf("Starting up on %d...\n", *port)
	s.Serve(l)
}
