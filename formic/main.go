package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	"github.com/creiht/formic/flother"
	pb "github.com/creiht/formic/proto"

	"net"
	"time"
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

func newApiServer(fs *InMemFS) *apiServer {
	s := new(apiServer)
	s.rpool = newRedisPool(*oortHost)
	s.fs = fs
	// TODO: Get epoch and node id from some config
	s.fl = flother.NewFlother(time.Time{}, 1)
	return s
}

func newRedisPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
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

	// TODO: Move this code into the InMemFS implementation
	fs := &InMemFS{nodes: make(map[uint64]*Entry)}
	// need to add root always
	n := &Entry{
		path:     "/",
		inode:    1,
		isdir:    true,
		entries:  make(map[string]uint64),
		ientries: make(map[uint64]string),
	}
	ts := time.Now().Unix()
	n.attr = &pb.Attr{
		Inode:  n.inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeDir | 0777),
	}
	fs.nodes[n.attr.Inode] = n

	s := grpc.NewServer(opts...)
	//pb.RegisterFileApiServer(s, newFileServer(fs))
	//pb.RegisterDirApiServer(s, newDirServer(fs))
	pb.RegisterApiServer(s, newApiServer(fs))
	grpclog.Printf("Starting up on %d...\n", *port)
	s.Serve(l)
}
