package main

import (
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
}

func NewTestFS() *TestFS {
	return &TestFS{}
}

func (fs *TestFS) GetChunk(id []byte) ([]byte, error) {
	return []byte(""), nil
}

func (fs *TestFS) WriteChunk(id, data []byte) error {
	return nil
}

func TestCreate(t *testing.T) {
	api := NewApiServer(NewTestDS(), NewTestFS())
	_, err := api.Create(context.Background(), &pb.DirEnt{Parent: 1, Name: "Test"})
	if err != nil {
		t.Error("Create Failed: ", err)
	}

}
