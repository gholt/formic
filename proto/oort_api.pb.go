// Code generated by protoc-gen-go.
// source: oort_api.proto
// DO NOT EDIT!

/*
Package proto is a generated protocol buffer package.

It is generated from these files:
	oort_api.proto

It has these top-level messages:
	Node
	LookupRequest
	Attr
	FileChunk
	WriteResponse
	DirEnt
	DirEntries
	SymlinkRequest
	ReadlinkResponse
	GetxattrRequest
	GetxattrResponse
	SetxattrRequest
	SetxattrResponse
	ListxattrRequest
	ListxattrResponse
	RemovexattrRequest
	RemovexattrResponse
*/
package proto

import proto1 "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto1.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// Node is the file inode
type Node struct {
	Inode uint64 `protobuf:"varint,1,opt,name=inode" json:"inode,omitempty"`
}

func (m *Node) Reset()         { *m = Node{} }
func (m *Node) String() string { return proto1.CompactTextString(m) }
func (*Node) ProtoMessage()    {}

// Lookup
type LookupRequest struct {
	Name   string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Parent uint64 `protobuf:"varint,2,opt,name=parent" json:"parent,omitempty"`
}

func (m *LookupRequest) Reset()         { *m = LookupRequest{} }
func (m *LookupRequest) String() string { return proto1.CompactTextString(m) }
func (*LookupRequest) ProtoMessage()    {}

// Attr. fields are optional by default in proto3, so
// clients don't have to send all fields when performing an
// attr update for example. These might not all be needed
// but i got tired of constantly forgetting fields.
type Attr struct {
	Inode  uint64 `protobuf:"varint,1,opt,name=inode" json:"inode,omitempty"`
	Atime  int64  `protobuf:"varint,2,opt,name=atime" json:"atime,omitempty"`
	Mtime  int64  `protobuf:"varint,3,opt,name=mtime" json:"mtime,omitempty"`
	Ctime  int64  `protobuf:"varint,4,opt,name=ctime" json:"ctime,omitempty"`
	Crtime int64  `protobuf:"varint,5,opt,name=crtime" json:"crtime,omitempty"`
	Mode   uint32 `protobuf:"varint,6,opt,name=mode" json:"mode,omitempty"`
	Valid  int32  `protobuf:"varint,7,opt,name=valid" json:"valid,omitempty"`
	Size   uint64 `protobuf:"varint,8,opt,name=size" json:"size,omitempty"`
}

func (m *Attr) Reset()         { *m = Attr{} }
func (m *Attr) String() string { return proto1.CompactTextString(m) }
func (*Attr) ProtoMessage()    {}

// WriteRequest
type FileChunk struct {
	Inode   uint64 `protobuf:"varint,1,opt,name=inode" json:"inode,omitempty"`
	Offset  int64  `protobuf:"varint,2,opt,name=offset" json:"offset,omitempty"`
	Payload []byte `protobuf:"bytes,3,opt,name=payload,proto3" json:"payload,omitempty"`
}

func (m *FileChunk) Reset()         { *m = FileChunk{} }
func (m *FileChunk) String() string { return proto1.CompactTextString(m) }
func (*FileChunk) ProtoMessage()    {}

// WriteRepsonse place holder. Maybe use an enum so
// we can map to fuse errors ?
type WriteResponse struct {
	Status int32 `protobuf:"varint,1,opt,name=status" json:"status,omitempty"`
}

func (m *WriteResponse) Reset()         { *m = WriteResponse{} }
func (m *WriteResponse) String() string { return proto1.CompactTextString(m) }
func (*WriteResponse) ProtoMessage()    {}

// DirEnt is a directory entry
type DirEnt struct {
	Name   string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Parent uint64 `protobuf:"varint,2,opt,name=parent" json:"parent,omitempty"`
	Attr   *Attr  `protobuf:"bytes,3,opt,name=attr" json:"attr,omitempty"`
}

func (m *DirEnt) Reset()         { *m = DirEnt{} }
func (m *DirEnt) String() string { return proto1.CompactTextString(m) }
func (*DirEnt) ProtoMessage()    {}

func (m *DirEnt) GetAttr() *Attr {
	if m != nil {
		return m.Attr
	}
	return nil
}

// DirEntries just contains a list of directory entries
type DirEntries struct {
	DirEntries  []*DirEnt `protobuf:"bytes,1,rep,name=DirEntries" json:"DirEntries,omitempty"`
	FileEntries []*DirEnt `protobuf:"bytes,2,rep,name=FileEntries" json:"FileEntries,omitempty"`
}

func (m *DirEntries) Reset()         { *m = DirEntries{} }
func (m *DirEntries) String() string { return proto1.CompactTextString(m) }
func (*DirEntries) ProtoMessage()    {}

func (m *DirEntries) GetDirEntries() []*DirEnt {
	if m != nil {
		return m.DirEntries
	}
	return nil
}

func (m *DirEntries) GetFileEntries() []*DirEnt {
	if m != nil {
		return m.FileEntries
	}
	return nil
}

// Symlink
type SymlinkRequest struct {
	Name   string `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Parent uint64 `protobuf:"varint,2,opt,name=parent" json:"parent,omitempty"`
	Target string `protobuf:"bytes,3,opt,name=target" json:"target,omitempty"`
}

func (m *SymlinkRequest) Reset()         { *m = SymlinkRequest{} }
func (m *SymlinkRequest) String() string { return proto1.CompactTextString(m) }
func (*SymlinkRequest) ProtoMessage()    {}

// Readlink
type ReadlinkResponse struct {
	Target string `protobuf:"bytes,1,opt,name=target" json:"target,omitempty"`
}

func (m *ReadlinkResponse) Reset()         { *m = ReadlinkResponse{} }
func (m *ReadlinkResponse) String() string { return proto1.CompactTextString(m) }
func (*ReadlinkResponse) ProtoMessage()    {}

// Getxattr
type GetxattrRequest struct {
	Inode    uint64 `protobuf:"varint,1,opt,name=inode" json:"inode,omitempty"`
	Name     string `protobuf:"bytes,2,opt,name=name" json:"name,omitempty"`
	Size     uint32 `protobuf:"varint,3,opt,name=size" json:"size,omitempty"`
	Position uint32 `protobuf:"varint,4,opt,name=position" json:"position,omitempty"`
}

func (m *GetxattrRequest) Reset()         { *m = GetxattrRequest{} }
func (m *GetxattrRequest) String() string { return proto1.CompactTextString(m) }
func (*GetxattrRequest) ProtoMessage()    {}

type GetxattrResponse struct {
	Xattr []byte `protobuf:"bytes,1,opt,name=xattr,proto3" json:"xattr,omitempty"`
}

func (m *GetxattrResponse) Reset()         { *m = GetxattrResponse{} }
func (m *GetxattrResponse) String() string { return proto1.CompactTextString(m) }
func (*GetxattrResponse) ProtoMessage()    {}

// Setxattr
type SetxattrRequest struct {
	Inode    uint64 `protobuf:"varint,1,opt,name=inode" json:"inode,omitempty"`
	Name     string `protobuf:"bytes,2,opt,name=name" json:"name,omitempty"`
	Xattr    []byte `protobuf:"bytes,3,opt,name=xattr,proto3" json:"xattr,omitempty"`
	Position uint32 `protobuf:"varint,4,opt,name=position" json:"position,omitempty"`
	Flags    uint32 `protobuf:"varint,5,opt,name=flags" json:"flags,omitempty"`
}

func (m *SetxattrRequest) Reset()         { *m = SetxattrRequest{} }
func (m *SetxattrRequest) String() string { return proto1.CompactTextString(m) }
func (*SetxattrRequest) ProtoMessage()    {}

type SetxattrResponse struct {
}

func (m *SetxattrResponse) Reset()         { *m = SetxattrResponse{} }
func (m *SetxattrResponse) String() string { return proto1.CompactTextString(m) }
func (*SetxattrResponse) ProtoMessage()    {}

// Listxattr
type ListxattrRequest struct {
	Inode    uint64 `protobuf:"varint,1,opt,name=inode" json:"inode,omitempty"`
	Size     uint32 `protobuf:"varint,2,opt,name=size" json:"size,omitempty"`
	Position uint32 `protobuf:"varint,3,opt,name=position" json:"position,omitempty"`
}

func (m *ListxattrRequest) Reset()         { *m = ListxattrRequest{} }
func (m *ListxattrRequest) String() string { return proto1.CompactTextString(m) }
func (*ListxattrRequest) ProtoMessage()    {}

type ListxattrResponse struct {
	Xattr []byte `protobuf:"bytes,1,opt,name=xattr,proto3" json:"xattr,omitempty"`
}

func (m *ListxattrResponse) Reset()         { *m = ListxattrResponse{} }
func (m *ListxattrResponse) String() string { return proto1.CompactTextString(m) }
func (*ListxattrResponse) ProtoMessage()    {}

// Removexattr
type RemovexattrRequest struct {
	Inode uint64 `protobuf:"varint,1,opt,name=inode" json:"inode,omitempty"`
	Name  string `protobuf:"bytes,2,opt,name=name" json:"name,omitempty"`
}

func (m *RemovexattrRequest) Reset()         { *m = RemovexattrRequest{} }
func (m *RemovexattrRequest) String() string { return proto1.CompactTextString(m) }
func (*RemovexattrRequest) ProtoMessage()    {}

type RemovexattrResponse struct {
}

func (m *RemovexattrResponse) Reset()         { *m = RemovexattrResponse{} }
func (m *RemovexattrResponse) String() string { return proto1.CompactTextString(m) }
func (*RemovexattrResponse) ProtoMessage()    {}

func init() {
	proto1.RegisterType((*Node)(nil), "proto.Node")
	proto1.RegisterType((*LookupRequest)(nil), "proto.LookupRequest")
	proto1.RegisterType((*Attr)(nil), "proto.Attr")
	proto1.RegisterType((*FileChunk)(nil), "proto.FileChunk")
	proto1.RegisterType((*WriteResponse)(nil), "proto.WriteResponse")
	proto1.RegisterType((*DirEnt)(nil), "proto.DirEnt")
	proto1.RegisterType((*DirEntries)(nil), "proto.DirEntries")
	proto1.RegisterType((*SymlinkRequest)(nil), "proto.SymlinkRequest")
	proto1.RegisterType((*ReadlinkResponse)(nil), "proto.ReadlinkResponse")
	proto1.RegisterType((*GetxattrRequest)(nil), "proto.GetxattrRequest")
	proto1.RegisterType((*GetxattrResponse)(nil), "proto.GetxattrResponse")
	proto1.RegisterType((*SetxattrRequest)(nil), "proto.SetxattrRequest")
	proto1.RegisterType((*SetxattrResponse)(nil), "proto.SetxattrResponse")
	proto1.RegisterType((*ListxattrRequest)(nil), "proto.ListxattrRequest")
	proto1.RegisterType((*ListxattrResponse)(nil), "proto.ListxattrResponse")
	proto1.RegisterType((*RemovexattrRequest)(nil), "proto.RemovexattrRequest")
	proto1.RegisterType((*RemovexattrResponse)(nil), "proto.RemovexattrResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// Client API for Api service

type ApiClient interface {
	SetAttr(ctx context.Context, in *Attr, opts ...grpc.CallOption) (*Attr, error)
	GetAttr(ctx context.Context, in *Node, opts ...grpc.CallOption) (*Attr, error)
	Read(ctx context.Context, in *Node, opts ...grpc.CallOption) (*FileChunk, error)
	Write(ctx context.Context, in *FileChunk, opts ...grpc.CallOption) (*WriteResponse, error)
	MkDir(ctx context.Context, in *DirEnt, opts ...grpc.CallOption) (*DirEnt, error)
	Create(ctx context.Context, in *DirEnt, opts ...grpc.CallOption) (*DirEnt, error)
	Remove(ctx context.Context, in *DirEnt, opts ...grpc.CallOption) (*WriteResponse, error)
	Lookup(ctx context.Context, in *LookupRequest, opts ...grpc.CallOption) (*DirEnt, error)
	ReadDirAll(ctx context.Context, in *Node, opts ...grpc.CallOption) (*DirEntries, error)
	Symlink(ctx context.Context, in *SymlinkRequest, opts ...grpc.CallOption) (*DirEnt, error)
	Readlink(ctx context.Context, in *Node, opts ...grpc.CallOption) (*ReadlinkResponse, error)
	Getxattr(ctx context.Context, in *GetxattrRequest, opts ...grpc.CallOption) (*GetxattrResponse, error)
	Setxattr(ctx context.Context, in *SetxattrRequest, opts ...grpc.CallOption) (*SetxattrResponse, error)
	Listxattr(ctx context.Context, in *ListxattrRequest, opts ...grpc.CallOption) (*ListxattrResponse, error)
	Removexattr(ctx context.Context, in *RemovexattrRequest, opts ...grpc.CallOption) (*RemovexattrResponse, error)
}

type apiClient struct {
	cc *grpc.ClientConn
}

func NewApiClient(cc *grpc.ClientConn) ApiClient {
	return &apiClient{cc}
}

func (c *apiClient) SetAttr(ctx context.Context, in *Attr, opts ...grpc.CallOption) (*Attr, error) {
	out := new(Attr)
	err := grpc.Invoke(ctx, "/proto.Api/SetAttr", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) GetAttr(ctx context.Context, in *Node, opts ...grpc.CallOption) (*Attr, error) {
	out := new(Attr)
	err := grpc.Invoke(ctx, "/proto.Api/GetAttr", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Read(ctx context.Context, in *Node, opts ...grpc.CallOption) (*FileChunk, error) {
	out := new(FileChunk)
	err := grpc.Invoke(ctx, "/proto.Api/Read", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Write(ctx context.Context, in *FileChunk, opts ...grpc.CallOption) (*WriteResponse, error) {
	out := new(WriteResponse)
	err := grpc.Invoke(ctx, "/proto.Api/Write", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) MkDir(ctx context.Context, in *DirEnt, opts ...grpc.CallOption) (*DirEnt, error) {
	out := new(DirEnt)
	err := grpc.Invoke(ctx, "/proto.Api/MkDir", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Create(ctx context.Context, in *DirEnt, opts ...grpc.CallOption) (*DirEnt, error) {
	out := new(DirEnt)
	err := grpc.Invoke(ctx, "/proto.Api/Create", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Remove(ctx context.Context, in *DirEnt, opts ...grpc.CallOption) (*WriteResponse, error) {
	out := new(WriteResponse)
	err := grpc.Invoke(ctx, "/proto.Api/Remove", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Lookup(ctx context.Context, in *LookupRequest, opts ...grpc.CallOption) (*DirEnt, error) {
	out := new(DirEnt)
	err := grpc.Invoke(ctx, "/proto.Api/Lookup", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) ReadDirAll(ctx context.Context, in *Node, opts ...grpc.CallOption) (*DirEntries, error) {
	out := new(DirEntries)
	err := grpc.Invoke(ctx, "/proto.Api/ReadDirAll", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Symlink(ctx context.Context, in *SymlinkRequest, opts ...grpc.CallOption) (*DirEnt, error) {
	out := new(DirEnt)
	err := grpc.Invoke(ctx, "/proto.Api/Symlink", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Readlink(ctx context.Context, in *Node, opts ...grpc.CallOption) (*ReadlinkResponse, error) {
	out := new(ReadlinkResponse)
	err := grpc.Invoke(ctx, "/proto.Api/Readlink", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Getxattr(ctx context.Context, in *GetxattrRequest, opts ...grpc.CallOption) (*GetxattrResponse, error) {
	out := new(GetxattrResponse)
	err := grpc.Invoke(ctx, "/proto.Api/Getxattr", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Setxattr(ctx context.Context, in *SetxattrRequest, opts ...grpc.CallOption) (*SetxattrResponse, error) {
	out := new(SetxattrResponse)
	err := grpc.Invoke(ctx, "/proto.Api/Setxattr", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Listxattr(ctx context.Context, in *ListxattrRequest, opts ...grpc.CallOption) (*ListxattrResponse, error) {
	out := new(ListxattrResponse)
	err := grpc.Invoke(ctx, "/proto.Api/Listxattr", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *apiClient) Removexattr(ctx context.Context, in *RemovexattrRequest, opts ...grpc.CallOption) (*RemovexattrResponse, error) {
	out := new(RemovexattrResponse)
	err := grpc.Invoke(ctx, "/proto.Api/Removexattr", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for Api service

type ApiServer interface {
	SetAttr(context.Context, *Attr) (*Attr, error)
	GetAttr(context.Context, *Node) (*Attr, error)
	Read(context.Context, *Node) (*FileChunk, error)
	Write(context.Context, *FileChunk) (*WriteResponse, error)
	MkDir(context.Context, *DirEnt) (*DirEnt, error)
	Create(context.Context, *DirEnt) (*DirEnt, error)
	Remove(context.Context, *DirEnt) (*WriteResponse, error)
	Lookup(context.Context, *LookupRequest) (*DirEnt, error)
	ReadDirAll(context.Context, *Node) (*DirEntries, error)
	Symlink(context.Context, *SymlinkRequest) (*DirEnt, error)
	Readlink(context.Context, *Node) (*ReadlinkResponse, error)
	Getxattr(context.Context, *GetxattrRequest) (*GetxattrResponse, error)
	Setxattr(context.Context, *SetxattrRequest) (*SetxattrResponse, error)
	Listxattr(context.Context, *ListxattrRequest) (*ListxattrResponse, error)
	Removexattr(context.Context, *RemovexattrRequest) (*RemovexattrResponse, error)
}

func RegisterApiServer(s *grpc.Server, srv ApiServer) {
	s.RegisterService(&_Api_serviceDesc, srv)
}

func _Api_SetAttr_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Attr)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).SetAttr(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_GetAttr_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Node)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).GetAttr(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Read_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Node)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Read(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Write_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(FileChunk)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Write(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_MkDir_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(DirEnt)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).MkDir(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(DirEnt)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Create(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Remove_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(DirEnt)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Remove(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Lookup_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(LookupRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Lookup(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_ReadDirAll_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Node)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).ReadDirAll(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Symlink_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(SymlinkRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Symlink(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Readlink_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Node)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Readlink(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Getxattr_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(GetxattrRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Getxattr(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Setxattr_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(SetxattrRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Setxattr(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Listxattr_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(ListxattrRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Listxattr(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _Api_Removexattr_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(RemovexattrRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(ApiServer).Removexattr(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _Api_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.Api",
	HandlerType: (*ApiServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SetAttr",
			Handler:    _Api_SetAttr_Handler,
		},
		{
			MethodName: "GetAttr",
			Handler:    _Api_GetAttr_Handler,
		},
		{
			MethodName: "Read",
			Handler:    _Api_Read_Handler,
		},
		{
			MethodName: "Write",
			Handler:    _Api_Write_Handler,
		},
		{
			MethodName: "MkDir",
			Handler:    _Api_MkDir_Handler,
		},
		{
			MethodName: "Create",
			Handler:    _Api_Create_Handler,
		},
		{
			MethodName: "Remove",
			Handler:    _Api_Remove_Handler,
		},
		{
			MethodName: "Lookup",
			Handler:    _Api_Lookup_Handler,
		},
		{
			MethodName: "ReadDirAll",
			Handler:    _Api_ReadDirAll_Handler,
		},
		{
			MethodName: "Symlink",
			Handler:    _Api_Symlink_Handler,
		},
		{
			MethodName: "Readlink",
			Handler:    _Api_Readlink_Handler,
		},
		{
			MethodName: "Getxattr",
			Handler:    _Api_Getxattr_Handler,
		},
		{
			MethodName: "Setxattr",
			Handler:    _Api_Setxattr_Handler,
		},
		{
			MethodName: "Listxattr",
			Handler:    _Api_Listxattr_Handler,
		},
		{
			MethodName: "Removexattr",
			Handler:    _Api_Removexattr_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}
