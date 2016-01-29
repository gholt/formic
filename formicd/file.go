package main

import (
	"crypto/tls"
	"errors"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"bazil.org/fuse"

	pb "github.com/creiht/formic/proto"
	"github.com/gholt/brimtime"
	"github.com/gogo/protobuf/proto"
	gp "github.com/pandemicsyn/oort/api/groupproto"
	vp "github.com/pandemicsyn/oort/api/valueproto"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type FileService interface {
	GetChunk(id []byte) ([]byte, error)
	WriteChunk(id, data []byte) error
}

var ErrStoreHasNewerValue = errors.New("Error store already has newer value")

type OortFS struct {
	vaddr              string
	gaddr              string
	gopts              []grpc.DialOption
	gcreds             credentials.TransportAuthenticator
	insecureSkipVerify bool
	vconn              *grpc.ClientConn
	gconn              *grpc.ClientConn
	vclient            vp.ValueStoreClient
	gclient            gp.GroupStoreClient
	sync.RWMutex
}

func NewOortFS(vaddr, gaddr string, insecureSkipVerify bool, grpcOpts ...grpc.DialOption) (*OortFS, error) {
	// TODO: This all eventually needs to replaced with value and group rings
	var err error
	o := &OortFS{
		vaddr: vaddr,
		gaddr: gaddr,
		gopts: grpcOpts,
		gcreds: credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: insecureSkipVerify,
		}),
		insecureSkipVerify: insecureSkipVerify,
	}
	o.gopts = append(o.gopts, grpc.WithTransportCredentials(o.gcreds))
	o.vconn, err = grpc.Dial(o.vaddr, o.gopts...)
	if err != nil {
		return &OortFS{}, err
	}
	o.vclient = vp.NewValueStoreClient(o.vconn)
	o.gconn, err = grpc.Dial(o.gaddr, o.gopts...)
	if err != nil {
		return &OortFS{}, err
	}
	o.gclient = gp.NewGroupStoreClient(o.gconn)
	// TODO: This should be setup out of band when an FS is first created
	// NOTE: This also means that it is only single user until we init filesystems out of band
	// Init the root node
	id := GetID(1, 1, 1, 0)
	n, err := o.GetChunk(id)
	if len(n) == 0 {
		log.Println("Root node not found, creating new root")
		// Need to create the root node
		r := &pb.InodeEntry{
			Path:  "/",
			Inode: 1,
			IsDir: true,
		}
		ts := time.Now().Unix()
		r.Attr = &pb.Attr{
			Inode:  1,
			Atime:  ts,
			Mtime:  ts,
			Ctime:  ts,
			Crtime: ts,
			Mode:   uint32(os.ModeDir | 0775),
			Uid:    1001, // TODO: need to config default user/group id
			Gid:    1001,
		}
		b, err := proto.Marshal(r)
		if err != nil {
			return &OortFS{}, err
		}
		err = o.WriteChunk(id, b)
		if err != nil {
			return &OortFS{}, err
		}
	}
	return o, nil
}

func (o *OortFS) ValueConnClose() error {
	o.Lock()
	defer o.Unlock()
	return o.vconn.Close()
}

func (o *OortFS) ValueConnState() (grpc.ConnectivityState, error) {
	o.RLock()
	defer o.RUnlock()
	return o.vconn.State()
}

func (o *OortFS) GetValueReadStream(ctx context.Context, opts ...grpc.CallOption) (vp.ValueStore_StreamReadClient, error) {
	o.RLock()
	defer o.RUnlock()
	return o.vclient.StreamRead(ctx)
}

func (o *OortFS) GetValueWriteStream(ctx context.Context, opts ...grpc.CallOption) (vp.ValueStore_StreamWriteClient, error) {
	o.RLock()
	defer o.RUnlock()
	return o.vclient.StreamWrite(ctx)
}

func (o *OortFS) GroupConnClose() error {
	o.Lock()
	defer o.Unlock()
	return o.gconn.Close()
}

func (o *OortFS) GroupConnState() (grpc.ConnectivityState, error) {
	o.RLock()
	defer o.RUnlock()
	return o.gconn.State()
}

func (o *OortFS) GetGroupReadStream(ctx context.Context, opts ...grpc.CallOption) (gp.GroupStore_StreamReadClient, error) {
	o.RLock()
	defer o.RUnlock()
	return o.gclient.StreamRead(ctx)
}

func (o *OortFS) GetGroupWriteStream(ctx context.Context, opts ...grpc.CallOption) (gp.GroupStore_StreamWriteClient, error) {
	o.RLock()
	defer o.RUnlock()
	return o.gclient.StreamWrite(ctx)
}

func (o *OortFS) GetChunk(id []byte) ([]byte, error) {
	stream, err := o.GetValueReadStream(context.Background())
	defer stream.CloseSend()
	if err != nil {
		return []byte(""), err
	}
	r := &vp.ReadRequest{}
	r.KeyA, r.KeyB = murmur3.Sum128(id)
	if err := stream.Send(r); err != nil {
		return []byte(""), err
	}
	res, err := stream.Recv()
	if err == io.EOF {
		return []byte(""), nil
	}
	if err != nil {
		return []byte(""), err
	}
	return res.Value, nil
}

func (o *OortFS) WriteChunk(id, data []byte) error {
	stream, err := o.GetValueWriteStream(context.Background())
	defer stream.CloseSend()
	if err != nil {
		return err
	}
	w := &vp.WriteRequest{
		Value: data,
	}
	w.KeyA, w.KeyB = murmur3.Sum128(id)
	w.Tsm = brimtime.TimeToUnixMicro(time.Now())
	if err := stream.Send(w); err != nil {
		return err
	}
	res, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	if res.Tsm > w.Tsm {
		return ErrStoreHasNewerValue
	}
	return nil
}

func (o *OortFS) GetAttr(id []byte) (*pb.Attr, error) {
	b, err := o.GetChunk(id)
	if err != nil {
		return &pb.Attr{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return &pb.Attr{}, err
	}
	return n.Attr, nil
}

func (o *OortFS) SetAttr(id []byte, attr *pb.Attr, v uint32) (*pb.Attr, error) {
	valid := fuse.SetattrValid(v)
	b, err := o.GetChunk(id)
	if err != nil {
		return &pb.Attr{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return &pb.Attr{}, err
	}
	if valid.Mode() {
		n.Attr.Mode = attr.Mode
	}
	if valid.Size() {
		if n.Attr.Size == 0 {
			n.Blocks = 0
			n.LastBlock = 0
		}
		n.Attr.Size = attr.Size
	}
	if valid.Mtime() {
		n.Attr.Mtime = attr.Mtime
	}
	if valid.Atime() {
		n.Attr.Atime = attr.Atime
	}
	if valid.Uid() {
		n.Attr.Uid = attr.Uid
	}
	if valid.Gid() {
		n.Attr.Gid = attr.Gid
	}
	b, err = proto.Marshal(n)
	if err != nil {
		return &pb.Attr{}, err
	}
	err = o.WriteChunk(id, b)
	if err != nil {
		return &pb.Attr{}, err
	}

	return n.Attr, nil
}

func (o *OortFS) Create(parent, id []byte, inode uint64, name string, attr *pb.Attr, isdir bool) (string, *pb.Attr, error) {
	// Check to see if the name already exists
	r := &gp.LookupRequest{}
	r.KeyA, r.KeyB = murmur3.Sum128(parent)
	r.NameKeyA, r.NameKeyB = murmur3.Sum128([]byte(name))
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	lres, err := o.gclient.Lookup(ctx, r)
	if err != nil {
		// TODO: Needs beter error handling
		return "", &pb.Attr{}, err
	}
	if lres.Err != "not found" { // TODO: figure out better error passing
		return "", &pb.Attr{}, nil
	}
	// Add the name to the group
	stream, err := o.GetGroupWriteStream(context.Background())
	defer stream.CloseSend()
	if err != nil {
		return "", &pb.Attr{}, err
	}
	w := &gp.WriteRequest{}
	w.KeyA, w.KeyB = murmur3.Sum128(parent)
	w.NameKeyA, w.NameKeyB = murmur3.Sum128([]byte(name))
	w.Tsm = brimtime.TimeToUnixMicro(time.Now())
	w.Value = id
	if err := stream.Send(w); err != nil {
		return "", &pb.Attr{}, err
	}
	wres, err := stream.Recv()
	if err != io.EOF && err != nil {
		return "", &pb.Attr{}, err
	}
	if wres.Tsm > w.Tsm {
		return "", &pb.Attr{}, ErrStoreHasNewerValue
	}
	// Add the inode entry
	n := &pb.InodeEntry{
		Path:   name,
		Inode:  inode,
		IsDir:  isdir,
		Attr:   attr,
		Blocks: 0,
	}
	b, err := proto.Marshal(n)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	err = o.WriteChunk(id, b)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	return name, attr, nil
}

func (o *OortFS) Lookup(parent []byte, name string) (string, *pb.Attr, error) {
	stream, err := o.GetGroupReadStream(context.Background())
	defer stream.CloseSend()
	if err != nil {
		return "", &pb.Attr{}, err
	}
	r := &gp.ReadRequest{}
	r.KeyA, r.KeyB = murmur3.Sum128(parent)
	r.NameKeyA, r.NameKeyB = murmur3.Sum128([]byte(name))
	if err := stream.Send(r); err != nil {
		return "", &pb.Attr{}, err
	}
	res, err := stream.Recv()
	if err == io.EOF {
		return "", &pb.Attr{}, nil
	}
	if err != nil {
		return "", &pb.Attr{}, err
	}
	b, err := o.GetChunk(res.Value)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return "", &pb.Attr{}, err
	}
	return n.Path, n.Attr, nil
}

// Needed to be able to sort the dirents
type ByDirent []*pb.DirEnt

func (d ByDirent) Len() int {
	return len(d)
}

func (d ByDirent) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d ByDirent) Less(i, j int) bool {
	return d[i].Name < d[j].Name
}

func (o *OortFS) ReadDirAll(id []byte) (*pb.ReadDirAllResponse, error) {

	// Get the keys from the group
	r := &gp.LookupGroupRequest{}
	r.A, r.B = murmur3.Sum128(id)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	lres, err := o.gclient.LookupGroup(ctx, r)
	if err != nil {
		// TODO: Needs beter error handling
		return &pb.ReadDirAllResponse{}, err
	}
	// Iterate over each key, getting the ID then the Inode Entry
	e := &pb.ReadDirAllResponse{}
	stream, err := o.GetGroupReadStream(context.Background())
	defer stream.CloseSend()
	lookup := &gp.ReadRequest{}
	lookup.KeyA, lookup.KeyB = murmur3.Sum128(id)
	for _, key := range lres.Items {
		// lookup the key in the group to get the id
		lookup.NameKeyA = key.NameKeyA
		lookup.NameKeyB = key.NameKeyB
		err := stream.Send(lookup)
		if err != nil {
			// TODO: Needs beter error handling
			continue
		}
		res, err := stream.Recv()
		if err != nil {
			continue
		}
		// get the inode entry
		b, err := o.GetChunk(res.Value)
		if err != nil {
			continue
		}
		n := &pb.InodeEntry{}
		err = proto.Unmarshal(b, n)
		if err != nil {
			continue
		}
		if n.IsDir {
			e.DirEntries = append(e.DirEntries, &pb.DirEnt{Name: n.Path, Attr: n.Attr})
		} else {
			e.FileEntries = append(e.FileEntries, &pb.DirEnt{Name: n.Path, Attr: n.Attr})
		}
	}
	sort.Sort(ByDirent(e.DirEntries))
	sort.Sort(ByDirent(e.FileEntries))
	return e, nil
}

func (o *OortFS) Remove(parent []byte, name string) (int32, error) {
	// Check to see if the name exists
	lr := &gp.LookupRequest{}
	lr.KeyA, lr.KeyB = murmur3.Sum128(parent)
	lr.NameKeyA, lr.NameKeyB = murmur3.Sum128([]byte(name))
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	lres, err := o.gclient.Lookup(ctx, lr)
	if err != nil {
		// TODO: Needs beter error handling
		return 1, err
	}
	if lres.Err != "not found" { // TODO: figure out better error passing
		return 1, nil
	}
	// Get the ID from the group list
	stream, err := o.GetGroupReadStream(context.Background())
	defer stream.CloseSend()
	if err != nil {
		return 1, err
	}
	rr := &gp.ReadRequest{}
	rr.KeyA, rr.KeyB = murmur3.Sum128(parent)
	rr.NameKeyA, rr.NameKeyB = murmur3.Sum128([]byte(name))
	if err := stream.Send(rr); err != nil {
		return 1, err
	}
	res, err := stream.Recv()
	if err != nil {
		return 1, err
	}
	// Remove the inode
	tsm := brimtime.TimeToUnixMicro(time.Now())
	vr := &vp.DeleteRequest{}
	vr.KeyA, vr.KeyB = murmur3.Sum128(res.Value)
	vr.Tsm = tsm
	ctx, _ = context.WithTimeout(context.Background(), time.Second*10)
	_, err = o.vclient.Delete(ctx, vr)
	if err != nil {
		return 1, err
	}
	// TODO: More error handling needed
	// Remove from the group
	gr := &gp.DeleteRequest{}
	gr.KeyA = rr.KeyA
	gr.KeyB = rr.KeyB
	gr.Tsm = tsm
	ctx, _ = context.WithTimeout(context.Background(), time.Second*10)
	_, err = o.gclient.Delete(ctx, gr)
	if err != nil {
		return 1, err // Not really sure what should be done here to try to recover from err
	}
	return 0, nil
}

func (o *OortFS) Update(id []byte, block, blocksize, size uint64, mtime int64) error {
	b, err := o.GetChunk(id)
	if err != nil {
		return err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return err
	}
	blocks := n.Blocks
	if block >= blocks {
		n.Blocks = block + 1
		n.LastBlock = size
		n.BlockSize = blocksize
		n.Attr.Size = blocksize*block + size
	} else if block == (blocks - 1) {
		n.LastBlock = size
		n.Attr.Size = blocksize*block + size
	}

	n.Attr.Mtime = mtime
	b, err = proto.Marshal(n)
	if err != nil {
		return err
	}
	err = o.WriteChunk(id, b)
	if err != nil {
		return err
	}
	return nil
}

func (o *OortFS) Symlink(parent, id []byte, name string, target string, attr *pb.Attr, inode uint64) (*pb.SymlinkResponse, error) {
	// Check to see if the name exists
	lr := &gp.LookupRequest{}
	lr.KeyA, lr.KeyB = murmur3.Sum128(parent)
	lr.NameKeyA, lr.NameKeyB = murmur3.Sum128([]byte(name))
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	lres, err := o.gclient.Lookup(ctx, lr)
	if err != nil {
		// TODO: Needs beter error handling
		return &pb.SymlinkResponse{}, err
	}
	if lres.Err != "not found" { // TODO: figure out better error passing
		// Exists
		return &pb.SymlinkResponse{}, nil
	}
	n := &pb.InodeEntry{
		Path:   name,
		Inode:  inode,
		IsDir:  false,
		IsLink: true,
		Target: target,
		Attr:   attr,
	}
	b, err := proto.Marshal(n)
	if err != nil {
		return &pb.SymlinkResponse{}, err
	}
	err = o.WriteChunk(id, b)
	if err != nil {
		return &pb.SymlinkResponse{}, err
	}
	// Add the name to the group
	stream, err := o.GetGroupWriteStream(context.Background())
	defer stream.CloseSend()
	if err != nil {
		return &pb.SymlinkResponse{}, err
	}
	w := &gp.WriteRequest{}
	w.KeyA, w.KeyB = murmur3.Sum128(parent)
	w.NameKeyA, w.NameKeyB = murmur3.Sum128([]byte(name))
	w.Tsm = brimtime.TimeToUnixMicro(time.Now())
	w.Value = id
	if err := stream.Send(w); err != nil {
		return &pb.SymlinkResponse{}, err
	}
	wres, err := stream.Recv()
	if err != io.EOF && err != nil {
		return &pb.SymlinkResponse{}, err
	}
	if wres.Tsm > w.Tsm {
		return &pb.SymlinkResponse{}, ErrStoreHasNewerValue
	}
	return &pb.SymlinkResponse{Name: name, Attr: attr}, nil
}

func (o *OortFS) Readlink(id []byte) (*pb.ReadlinkResponse, error) {
	b, err := o.GetChunk(id)
	if err != nil {
		return &pb.ReadlinkResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return &pb.ReadlinkResponse{}, err
	}
	return &pb.ReadlinkResponse{Target: n.Target}, nil
}

func (o *OortFS) Getxattr(id []byte, name string) (*pb.GetxattrResponse, error) {
	b, err := o.GetChunk(id)
	if err != nil {
		return &pb.GetxattrResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return &pb.GetxattrResponse{}, err
	}
	if xattr, ok := n.Xattr[name]; ok {
		return &pb.GetxattrResponse{Xattr: xattr}, nil
	}
	return &pb.GetxattrResponse{}, nil
}

func (o *OortFS) Setxattr(id []byte, name string, value []byte) (*pb.SetxattrResponse, error) {
	b, err := o.GetChunk(id)
	if err != nil {
		return &pb.SetxattrResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return &pb.SetxattrResponse{}, err
	}
	n.Xattr[name] = value
	b, err = proto.Marshal(n)
	if err != nil {
		return &pb.SetxattrResponse{}, err
	}
	err = o.WriteChunk(id, b)
	if err != nil {
		return &pb.SetxattrResponse{}, err
	}
	return &pb.SetxattrResponse{}, nil
}

func (o *OortFS) Listxattr(id []byte) (*pb.ListxattrResponse, error) {
	resp := &pb.ListxattrResponse{}
	b, err := o.GetChunk(id)
	if err != nil {
		return &pb.ListxattrResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return &pb.ListxattrResponse{}, err
	}
	names := ""
	for name := range n.Xattr {
		names += name
		names += "\x00"
	}
	resp.Xattr = []byte(names)
	return resp, nil
}

func (o *OortFS) Removexattr(id []byte, name string) (*pb.RemovexattrResponse, error) {
	b, err := o.GetChunk(id)
	if err != nil {
		return &pb.RemovexattrResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return &pb.RemovexattrResponse{}, err
	}
	delete(n.Xattr, name)
	b, err = proto.Marshal(n)
	if err != nil {
		return &pb.RemovexattrResponse{}, err
	}
	err = o.WriteChunk(id, b)
	if err != nil {
		return &pb.RemovexattrResponse{}, err
	}
	return &pb.RemovexattrResponse{}, nil
}

func (o *OortFS) Rename(oldParent, newParent []byte, oldName, newName string) (*pb.RenameResponse, error) {
	// Check to see if the name exists
	lr := &gp.LookupRequest{}
	lr.KeyA, lr.KeyB = murmur3.Sum128(oldParent)
	lr.NameKeyA, lr.NameKeyB = murmur3.Sum128([]byte(oldName))
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	lres, err := o.gclient.Lookup(ctx, lr)
	if err != nil {
		// TODO: Needs beter error handling
		return &pb.RenameResponse{}, err
	}
	if lres.Err != "" { // TODO: figure out better error passing
		return &pb.RenameResponse{}, nil
	}
	// Check if the new name already exists
	lr.KeyA, lr.KeyB = murmur3.Sum128(newParent)
	lr.NameKeyA, lr.NameKeyB = murmur3.Sum128([]byte(newName))
	ctx, _ = context.WithTimeout(context.Background(), time.Second*10)
	lres, err = o.gclient.Lookup(ctx, lr)
	if err != nil {
		// TODO: Needs beter error handling
		return &pb.RenameResponse{}, err
	}
	if lres.Err != "not found" { // TODO: figure out better error passing
		// Exists
		return &pb.RenameResponse{}, nil
	}
	// Get the ID from the group list
	rstream, err := o.GetGroupReadStream(context.Background())
	defer rstream.CloseSend()
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	rr := &gp.ReadRequest{}
	rr.KeyA, rr.KeyB = murmur3.Sum128(oldParent)
	rr.NameKeyA, rr.NameKeyB = murmur3.Sum128([]byte(oldName))
	if err := rstream.Send(rr); err != nil {
		return &pb.RenameResponse{}, err
	}
	res, err := rstream.Recv()
	id := res.Value
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	// Delete old entry
	gr := &gp.DeleteRequest{}
	gr.KeyA = rr.KeyA
	gr.KeyB = rr.KeyB
	gr.Tsm = brimtime.TimeToUnixMicro(time.Now())
	ctx, _ = context.WithTimeout(context.Background(), time.Second*10)
	_, err = o.gclient.Delete(ctx, gr)
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	// Create new entry
	// TODO: Figure out the right ordering for all of this and of course err recovery
	wstream, err := o.GetGroupWriteStream(context.Background())
	defer wstream.CloseSend()
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	w := &gp.WriteRequest{}
	w.KeyA, w.KeyB = murmur3.Sum128(newParent)
	w.NameKeyA, w.NameKeyB = murmur3.Sum128([]byte(newName))
	w.Tsm = brimtime.TimeToUnixMicro(time.Now())
	w.Value = id
	if err := wstream.Send(w); err != nil {
		return &pb.RenameResponse{}, err
	}
	wres, err := wstream.Recv()
	if err != io.EOF && err != nil {
		return &pb.RenameResponse{}, err
	}
	if wres.Tsm > w.Tsm {
		return &pb.RenameResponse{}, ErrStoreHasNewerValue
	}
	// Update inode info
	b, err := o.GetChunk(id)
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	n := &pb.InodeEntry{}
	err = proto.Unmarshal(b, n)
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	n.Path = newName
	b, err = proto.Marshal(n)
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	err = o.WriteChunk(id, b)
	if err != nil {
		return &pb.RenameResponse{}, err
	}
	return &pb.RenameResponse{}, nil
}
