package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"

	pb "github.com/creiht/formic/proto"
	"github.com/pandemicsyn/ftls"
	"github.com/pandemicsyn/oort/api"

	"net"
)

var (
	usetls              = flag.Bool("tls", true, "Connection uses TLS if true, else plain TCP")
	certFile            = flag.String("cert_file", "/var/lib/formic/server.crt", "The TLS cert file")
	keyFile             = flag.String("key_file", "/var/lib/formic/server.key", "The TLS key file")
	port                = flag.Int("port", 8445, "The server port")
	oortValueSyndicate  = flag.String("oortvaluesyndicate", "", "Syndicate server for value store information")
	oortGroupSyndicate  = flag.String("oortgroupsyndicate", "", "Syndicate server for group store information")
	oortValueRing       = flag.String("oortvaluering", "/var/lib/formic/ring/valuestore.ring", "Location of cached value store ring file")
	oortGroupRing       = flag.String("oortgroupring", "/var/lib/formic/ring/groupstore.ring", "Location of cached value store ring file")
	insecureSkipVerify  = flag.Bool("skipverify", false, "don't verify cert")
	oortClientMutualTLS = flag.Bool("mutualtls", true, "whether or not the server expects mutual tls auth")
	oortClientCert      = flag.String("oort-client-cert", "/var/lib/formic/client.crt", "cert file to use")
	oortClientKey       = flag.String("oort-client-key", "/var/lib/formic/client.key", "key file to use")
	oortClientCA        = flag.String("oort-client-ca", "/var/lib/formic/ca.pem", "ca file to use")
)

// FatalIf is just a lazy log/panic on error func
func FatalIf(err error, msg string) {
	if err != nil {
		grpclog.Fatalf("%s: %v", msg, err)
	}
}

func main() {
	flag.Parse()

	envtls := os.Getenv("FORMICD_TLS")
	if envtls == "true" {
		*usetls = true
	}

	envoortvsyndicate := os.Getenv("FORMICD_OORT_VALUE_SYNDICATE")
	if envoortvsyndicate != "" {
		*oortValueSyndicate = envoortvsyndicate
	}

	envoortgsyndicate := os.Getenv("FORMICD_OORT_GROUP_SYNDICATE")
	if envoortgsyndicate != "" {
		*oortGroupSyndicate = envoortgsyndicate
	}

	envoortvring := os.Getenv("FORMICD_OORT_VALUE_RING")
	if envoortvring != "" {
		*oortValueRing = envoortvring
	}

	envoortgring := os.Getenv("FORMICD_OORT_GROUP_RING")
	if envoortgring != "" {
		*oortGroupRing = envoortgring
	}

	envport := os.Getenv("FORMICD_PORT")
	if envport != "" {
		p, err := strconv.Atoi(envport)
		if err != nil {
			log.Println("Did not send valid port from env:", err)
		} else {
			*port = p
		}
	}

	envcert := os.Getenv("FORMICD_CERT_FILE")
	if envcert != "" {
		*certFile = envcert
	}

	envkey := os.Getenv("FORMICD_KEY_FILE")
	if envkey != "" {
		*keyFile = envkey
	}
	envSkipVerify := os.Getenv("FORMICD_INSECURE_SKIP_VERIFY")
	if envSkipVerify == "true" {
		*insecureSkipVerify = true
	}
	envMutualTLS := os.Getenv("FORMICD_MUTUAL_TLS")
	if envMutualTLS == "true" {
		*oortClientMutualTLS = true
	}
	envClientCA := os.Getenv("FORMICD_CLIENT_CA_FILE")
	if envClientCA != "" {
		*oortClientCA = envClientCA
	}
	envClientCert := os.Getenv("FORMICD_CLIENT_CERT_FILE")
	if envClientCert != "" {
		*oortClientCert = envClientCert
	}
	envClientKey := os.Getenv("FORMICD_CLIENT_KEY_FILE")
	if envClientKey != "" {
		*oortClientKey = envClientKey
	}

	var opts []grpc.ServerOption
	if *usetls {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		FatalIf(err, "Couldn't load cert from file")
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	s := grpc.NewServer(opts...)
	copt, err := ftls.NewGRPCClientDialOpt(&ftls.Config{
		MutualTLS:          *oortClientMutualTLS,
		InsecureSkipVerify: *insecureSkipVerify,
		CertFile:           *oortClientCert,
		KeyFile:            *oortClientKey,
		CAFile:             *oortClientCA,
	})
	if err != nil {
		grpclog.Fatalln("Cannot setup tls config:", err)
	}

	vstore := api.NewReplValueStore(&api.ReplValueStoreConfig{
		AddressIndex:       2,
		GRPCOpts:           []grpc.DialOption{copt},
		RingServer:         *oortValueSyndicate,
		RingCachePath:      *oortValueRing,
		RingServerGRPCOpts: []grpc.DialOption{copt},
	})
	if err := vstore.Startup(context.Background()); err != nil {
		grpclog.Fatalln("Cannot start valuestore connector:", err)
	}

	gstore := api.NewReplGroupStore(&api.ReplGroupStoreConfig{
		AddressIndex:       2,
		GRPCOpts:           []grpc.DialOption{copt},
		RingServer:         *oortGroupSyndicate,
		RingCachePath:      *oortGroupRing,
		RingServerGRPCOpts: []grpc.DialOption{copt},
	})
	if err := gstore.Startup(context.Background()); err != nil {
		grpclog.Fatalln("Cannot start valuestore connector:", err)
	}

	fs, err := NewOortFS(vstore, gstore)
	if err != nil {
		grpclog.Fatalln(err)
	}
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	FatalIf(err, "Failed to bind to port")
	pb.RegisterApiServer(s, NewApiServer(fs))
	grpclog.Printf("Starting up on %d...\n", *port)
	s.Serve(l)
}
