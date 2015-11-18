package main

import (
	"bytes"
	"fmt"
	"testing"

	"golang.org/x/net/context"

	pb "github.com/creiht/formic/proto"
)

// Minimal DirService for testing
type TestDS struct {
}

func NewTestDS() *TestDS {
	return &TestDS{}
}

func (ds *TestDS) GetAttr(inode uint64) (*pb.Attr, error) {
	return &pb.Attr{}, nil
}

func (ds *TestDS) SetAttr(inode uint64, attr *pb.SetAttrRequest) (*pb.Attr, error) {
	return &pb.Attr{}, nil
}

func (ds *TestDS) Create(parent, inode uint64, name string, attr *pb.Attr, isdir bool) (*pb.DirEnt, error) {
	return &pb.DirEnt{Name: name, Attr: attr}, nil
}

func (ds *TestDS) Lookup(parent uint64, name string) (*pb.DirEnt, error) {
	return &pb.DirEnt{}, nil
}

func (ds *TestDS) ReadDirAll(inode uint64) (*pb.DirEntries, error) {
	return &pb.DirEntries{}, nil
}

func (ds *TestDS) Remove(parent uint64, name string) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{Status: 1}, nil
}

func (ds *TestDS) Update(inode, size uint64, mtime int64) {
}

func (ds *TestDS) Symlink(parent uint64, name string, target string, attr *pb.Attr, inode uint64) (*pb.DirEnt, error) {
	return &pb.DirEnt{}, nil
}

func (ds *TestDS) Readlink(inode uint64) (*pb.ReadlinkResponse, error) {
	return &pb.ReadlinkResponse{}, nil
}

func (ds *TestDS) Getxattr(r *pb.GetxattrRequest) (*pb.GetxattrResponse, error) {
	return &pb.GetxattrResponse{}, nil
}

func (ds *TestDS) Setxattr(r *pb.SetxattrRequest) (*pb.SetxattrResponse, error) {
	return &pb.SetxattrResponse{}, nil
}

func (ds *TestDS) Listxattr(r *pb.ListxattrRequest) (*pb.ListxattrResponse, error) {
	return &pb.ListxattrResponse{}, nil
}

func (ds *TestDS) Removexattr(r *pb.RemovexattrRequest) (*pb.RemovexattrResponse, error) {
	return &pb.RemovexattrResponse{}, nil
}

func (ds *TestDS) Rename(r *pb.RenameRequest) (*pb.RenameResponse, error) {
	return &pb.RenameResponse{}, nil
}

// Minimal FileService for testing
type TestFS struct {
	writes [][]byte
}

func NewTestFS() *TestFS {
	return &TestFS{
		writes: make([][]byte, 0),
	}
}

func (fs *TestFS) GetChunk(id []byte) ([]byte, error) {
	return []byte(""), nil
}

func (fs *TestFS) WriteChunk(id, data []byte) error {
	fs.writes = append(fs.writes, data)
	return nil
}

func (fs *TestFS) clearwrites() {
	fs.writes = make([][]byte, 0)
}

func TestCreate(t *testing.T) {
	api := NewApiServer(NewTestDS(), NewTestFS())
	_, err := api.Create(context.Background(), &pb.DirEnt{Parent: 1, Name: "Test"})
	if err != nil {
		t.Error("Create Failed: ", err)
	}

}

func TestWrite_Basic(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(NewTestDS(), fs)
	api.blocksize = 10
	chunk := pb.FileChunk{
		Offset:  0,
		Payload: []byte("1234567890"),
		Inode:   0,
	}
	r, err := api.Write(context.Background(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload, fs.writes[0]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload, fs.writes[0])
	}
	chunk.Payload = []byte("1")
	fs.clearwrites()
	r, err = api.Write(context.Background(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload, fs.writes[0]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload, fs.writes[0])
	}

}

func TestWrite_Chunk(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(NewTestDS(), fs)
	api.blocksize = 5
	chunk := pb.FileChunk{
		Offset:  0,
		Payload: []byte("1234567890"),
		Inode:   0,
	}
	r, err := api.Write(context.Background(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload[:5], fs.writes[0]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload[:5], fs.writes[0])
	}
	if !bytes.Equal(chunk.Payload[5:], fs.writes[1]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload[5:], fs.writes[1])
	}
}

func TestWrite_Offset(t *testing.T) {
	fs := NewTestFS()
	api := NewApiServer(NewTestDS(), fs)
	api.blocksize = 10
	chunk := pb.FileChunk{
		Offset:  5,
		Payload: []byte("12345"),
		Inode:   0,
	}
	r, err := api.Write(context.Background(), &chunk)
	if err != nil {
		t.Error("Write Failed: ", err)
	}
	if r.Status != 0 {
		t.Error("Write status expected: 0, received: ", r.Status)
	}
	if !bytes.Equal(chunk.Payload, fs.writes[0][5:]) {
		fmt.Println(fs.writes)
		t.Errorf("Expected write: '%s' recieved: '%s'", chunk.Payload, fs.writes[0][5:])
	}
}
