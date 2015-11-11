package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"time"

	"github.com/creiht/formic/flother"
	pb "github.com/creiht/formic/proto"
	"github.com/garyburd/redigo/redis"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
)

type apiServer struct {
	sync.RWMutex
	ds        DirService
	fs        FileService
	fl        *flother.Flother
	blocksize int64
}

func NewApiServer(ds DirService, fs FileService) *apiServer {
	s := new(apiServer)
	s.ds = ds
	s.fs = fs
	// TODO: Get epoch and node id from some config
	s.fl = flother.NewFlother(time.Time{}, 1)
	s.blocksize = int64(1024 * 64) // Default Block Size (64K)
	return s
}

func (s *apiServer) GetID(custID, shareID, inode, block uint64) []byte {
	// TODO: Figure out what arrangement we want to use for the hash
	h := murmur3.New128()
	binary.Write(h, binary.BigEndian, custID)
	binary.Write(h, binary.BigEndian, shareID)
	binary.Write(h, binary.BigEndian, inode)
	binary.Write(h, binary.BigEndian, block)
	s1, s2 := h.Sum128()
	id := make([]byte, 8)
	b := bytes.NewBuffer(id)
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return id
}

func (s *apiServer) GetAttr(ctx context.Context, r *pb.Node) (*pb.Attr, error) {
	return s.ds.GetAttr(r.Inode)
}

func (s *apiServer) SetAttr(ctx context.Context, r *pb.SetAttrRequest) (*pb.Attr, error) {
	return s.ds.SetAttr(r.Inode, r)
}

func (s *apiServer) Create(ctx context.Context, r *pb.DirEnt) (*pb.DirEnt, error) {
	ts := time.Now().Unix()
	inode := s.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(0777),
	}
	return s.ds.Create(r.Parent, inode, r.Name, attr, false)
}

func (s *apiServer) MkDir(ctx context.Context, r *pb.DirEnt) (*pb.DirEnt, error) {
	ts := time.Now().Unix()
	inode := s.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeDir | 0777),
	}
	return s.ds.Create(r.Parent, inode, r.Name, attr, true)
}

func (s *apiServer) Read(ctx context.Context, r *pb.Node) (*pb.FileChunk, error) {
	var err error
	// TODO: Add support for reading blocks
	id := s.GetID(1, 1, r.Inode, uint64(0))
	data, err := s.fs.GetChunk(id)
	if err != nil {
		if err == redis.ErrNil {
			//file is empty or doesn't exist yet.
			return &pb.FileChunk{}, nil
		}
		return &pb.FileChunk{}, err
	}
	f := &pb.FileChunk{Inode: r.Inode, Payload: data}
	return f, nil
}

func min(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func (s *apiServer) Write(ctx context.Context, r *pb.FileChunk) (*pb.WriteResponse, error) {
	block := uint64(r.Offset / s.blocksize)
	// TODO: Handle unaligned offsets
	/*	firstOffset := int64(0)
		if r.Offset%s.blocksize != 0 {
			// Handle non-aligned offset
			firstOffset = r.Offset - int64(block)*s.blocksize
		} */
	cur := int64(0)
	for cur < int64(len(r.Payload)) {
		sendSize := min(s.blocksize, int64(len(r.Payload))-cur)
		payload := r.Payload[cur : cur+sendSize]
		id := s.GetID(1, 1, r.Inode, block)
		if sendSize < s.blocksize {
			// need to get the block and update
			data, err := s.fs.GetChunk(id)
			// TODO: Need better error handling for when there is a block but it can't retreive it
			if err != nil && len(payload) < len(data) {
				copy(data, payload)
				payload = data
			}
		}
		err := s.fs.WriteChunk(id, r.Payload[cur:cur+sendSize])
		// TODO: Need better error handling for failing with multiple chunks
		if err != nil {
			return &pb.WriteResponse{Status: 1}, err
		}
		s.ds.Update(r.Inode, uint64(len(r.Payload)), time.Now().Unix())
		cur += sendSize
	}
	return &pb.WriteResponse{Status: 0}, nil
}

func (s *apiServer) Lookup(ctx context.Context, r *pb.LookupRequest) (*pb.DirEnt, error) {
	return s.ds.Lookup(r.Parent, r.Name)
}

func (s *apiServer) ReadDirAll(ctx context.Context, n *pb.Node) (*pb.DirEntries, error) {
	return s.ds.ReadDirAll(n.Inode)
}

func (s *apiServer) Remove(ctx context.Context, r *pb.DirEnt) (*pb.WriteResponse, error) {
	// TODO: Add calls to remove from backing store
	return s.ds.Remove(r.Parent, r.Name)
}

func (s *apiServer) Symlink(ctx context.Context, r *pb.SymlinkRequest) (*pb.DirEnt, error) {
	ts := time.Now().Unix()
	inode := s.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeSymlink | 0777),
		Size:   uint64(len(r.Target)),
	}
	return s.ds.Symlink(r.Parent, r.Name, r.Target, attr, inode)
}

func (s *apiServer) Readlink(ctx context.Context, n *pb.Node) (*pb.ReadlinkResponse, error) {
	return s.ds.Readlink(n.Inode)
}

func (s *apiServer) Getxattr(ctx context.Context, r *pb.GetxattrRequest) (*pb.GetxattrResponse, error) {
	return s.ds.Getxattr(r)
}

func (s *apiServer) Setxattr(ctx context.Context, r *pb.SetxattrRequest) (*pb.SetxattrResponse, error) {
	return s.ds.Setxattr(r)
}

func (s *apiServer) Listxattr(ctx context.Context, r *pb.ListxattrRequest) (*pb.ListxattrResponse, error) {
	return s.ds.Listxattr(r)
}

func (s *apiServer) Removexattr(ctx context.Context, r *pb.RemovexattrRequest) (*pb.RemovexattrResponse, error) {
	return s.ds.Removexattr(r)
}

func (s *apiServer) Rename(ctx context.Context, r *pb.RenameRequest) (*pb.RenameResponse, error) {
	return s.ds.Rename(r)
}

func (s *apiServer) Statfs(ctx context.Context, r *pb.StatfsRequest) (*pb.StatfsResponse, error) {
	resp := &pb.StatfsResponse{
		Blocks:  281474976710656, // 1 exabyte (asuming 4K block size)
		Bfree:   281474976710656,
		Bavail:  281474976710656,
		Files:   1000000000000, // 1 trillion inodes
		Ffree:   1000000000000,
		Bsize:   4096, // it looked like ext4 used 4KB blocks
		Namelen: 256,
		Frsize:  4096, // this should probably match Bsize so we don't allow fragmented blocks
	}
	return resp, nil
}
