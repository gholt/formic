package main

import (
	"fmt"
	"net"
	"os"
	"path"
	"sync"

	pb "github.com/creiht/formic/proto"
	"github.com/gholt/flog"
	"github.com/pandemicsyn/ftls"
	"github.com/pandemicsyn/oort/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/grpclog"
)

// Service is what the CmdCtrl.Server will control.
type service struct {
	path               string
	port               int
	insecureSkipVerify bool
	skipMutualTLS      bool
	oortValueSyndicate string
	oortGroupSyndicate string

	lock     sync.Mutex
	doneChan chan struct{}
	listener net.Listener
	server   *grpc.Server
}

func newService(c *config) *service {
	c = resolveConfig(c)
	return &service{
		path:               c.path,
		port:               c.port,
		insecureSkipVerify: c.insecureSkipVerify,
		skipMutualTLS:      c.skipMutualTLS,
		oortValueSyndicate: c.oortValueSyndicate,
		oortGroupSyndicate: c.oortGroupSyndicate,
	}
}

// Start starts the service and returns a channel that is closed when the
// service stops. This channel should be used just for this launching of the
// service. For example, two Start() calls with no Stop() might return the same
// channel, but after Stop() the next Start() should return a new channel
// (since the first channel should have been closed).
func (s *service) Start() <-chan struct{} {
	var retChan chan struct{}
	s.lock.Lock()
	if s.doneChan != nil {
		select {
		case <-s.doneChan: // Means previous run has stopped
			s.doneChan = nil
			s.listener = nil
			s.server = nil
		default: // Means previous run is still going
		}
	}
	if s.doneChan == nil {
		var opts []grpc.ServerOption
		if creds, err := credentials.NewServerTLSFromFile(path.Join(s.path, "server.crt"), path.Join(s.path, "server.key")); err != nil {
			flog.CriticalPrintln("Couldn't load cert from file:", err)
		} else {
			opts = []grpc.ServerOption{grpc.Creds(creds)}
			s.server = grpc.NewServer(opts...)
			copt, err := ftls.NewGRPCClientDialOpt(&ftls.Config{
				MutualTLS:          !s.skipMutualTLS,
				InsecureSkipVerify: s.insecureSkipVerify,
				CertFile:           path.Join(s.path, "client.crt"),
				KeyFile:            path.Join(s.path, "client.key"),
				CAFile:             path.Join(s.path, "ca.pem"),
			})
			if err != nil {
				flog.CriticalPrintln("Cannot setup TLS config:", err)
			} else {
				clientID, _ := os.Hostname()
				if clientID != "" {
					clientID += "/formicd"
				}
				vstore := api.NewReplValueStore(&api.ReplValueStoreConfig{
					AddressIndex:       2,
					GRPCOpts:           []grpc.DialOption{copt},
					RingServer:         s.oortValueSyndicate,
					RingCachePath:      path.Join(s.path, "ring/valuestore.ring"),
					RingServerGRPCOpts: []grpc.DialOption{copt},
					RingClientID:       clientID,
				})
				if err := vstore.Startup(context.Background()); err != nil {
					grpclog.Fatalln("Cannot start valuestore connector:", err)
				}
				gstore := api.NewReplGroupStore(&api.ReplGroupStoreConfig{
					AddressIndex:       2,
					GRPCOpts:           []grpc.DialOption{copt},
					RingServer:         s.oortGroupSyndicate,
					RingCachePath:      path.Join(s.path, "ring/groupstore.ring"),
					RingServerGRPCOpts: []grpc.DialOption{copt},
					RingClientID:       clientID,
				})
				if err := gstore.Startup(context.Background()); err != nil {
					grpclog.Fatalln("Cannot start valuestore connector:", err)
				}
				if fs, err := NewOortFS(vstore, gstore); err != nil {
					flog.CriticalPrintln("Cannot start OortFS:", err)
				} else {
					s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.port))
					if err != nil {
						flog.CriticalPrintln("Failed to bind to port:", err)
					} else {
						pb.RegisterApiServer(s.server, NewApiServer(fs))
						flog.DebugPrintf("Started up on %d...", s.port)
						s.doneChan = make(chan struct{})
						go func(g *grpc.Server, l net.Listener, c chan<- struct{}) {
							if err := g.Serve(l); err != nil {
								flog.ErrorPrintln("Formic GRPC Server error:", err)
							}
							g.Stop()
							l.Close()
							close(c)
						}(s.server, s.listener, s.doneChan)
					}
				}
			}
		}
	}
	retChan = s.doneChan
	if retChan == nil {
		retChan = make(chan struct{})
		close(retChan)
	}
	s.lock.Unlock()
	return retChan
}

// Stop stops the service and closes any channel Start() previously returned.
func (s *service) Stop() {
	s.lock.Lock()
	if s.doneChan != nil {
		s.server.Stop()
		s.listener.Close()
		<-s.doneChan
		s.doneChan = nil
		s.server = nil
		s.listener = nil
	}
	s.lock.Unlock()
}

// RingUpdate notifies the service of a new ring; the service should return the
// version of the ring that will be in use after this call, which might be
// newer than the ring given to this call, but should not be older.
func (s *service) RingUpdate(version int64, ringBytes []byte) int64 {
	// TODO
	return version
}

// Stats asks the service to return the current values of any metrics it's
// tracking.
func (s *service) Stats() []byte {
	// TODO
	return []byte{}
}

// HealthCheck returns true if running, or false and a message as to the reason
// why not.
func (s *service) HealthCheck() (bool, string) {
	var r bool
	var m string
	s.lock.Lock()
	if s.doneChan != nil {
		r = true
	} else {
		r = false
		m = "not running"
	}
	s.lock.Unlock()
	return r, m
}
