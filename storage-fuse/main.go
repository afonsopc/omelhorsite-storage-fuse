package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

type File struct {
	Name         string `json:"name"`
	PresignedUrl string `json:"presignedUrl"`
	Size         uint64 `json:"size"`
}

type Folder struct {
	Name string `json:"name"`
}

type listDirectoryResponse struct {
	Folders []Folder `json:"folders"`
	Files   []File   `json:"files"`
}

func listDirectory(key string, path string) (listDirectoryResponse, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", "https://storage.omelhorsite.pt/storage/list", nil)

	if err != nil {
		return listDirectoryResponse{}, err
	}

	q := url.Values{}
	q.Add("key", key)
	q.Add("path", path)

	req.URL.RawQuery = q.Encode()

	req.Header.Set("Authorization", "Bearer "+os.Getenv("ACCOUNT_TOKEN"))

	resp, err := client.Do(req)

	if err != nil {
		return listDirectoryResponse{}, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return listDirectoryResponse{}, err
	}

	if resp.StatusCode != 200 {
		return listDirectoryResponse{}, fmt.Errorf("failed to list folder: %s | %s", resp.Status, body)
	}

	var dir listDirectoryResponse
	err = json.Unmarshal(body, &dir)
	if err != nil {
		return listDirectoryResponse{}, err
	}

	return dir, nil
}

func getFile(url string) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get file: %s | %s", resp.Status, body)
	}

	return body, nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	fuse.Unmount(mountpoint)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("omsstorage"),
		fuse.Subtype("omsstoragefs"),
		fuse.AllowOther(),
	)
	if err != nil {
		log.Fatal(err)
	}

	defer c.Close()

	err = fs.Serve(c, FS{})
	if err != nil {
		log.Fatal(err)
	}
}

type FS struct{}

type Directory struct {
	Path string
}

func (FS) Root() (fs.Node, error) {
	return Directory{
		Path: "/",
	}, nil
}

func (Directory) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0o555
	return nil
}

func (d Directory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	dir, err := listDirectory("mine", d.Path)

	log.Println("Looking up:", name)

	if err != nil {
		return nil, err
	}

	for _, file := range dir.Files {
		if file.Name == name {
			return &file, nil
		}
	}

	for _, folder := range dir.Folders {
		if folder.Name == name {
			return &Directory{Path: filepath.Join(d.Path, name)}, nil
		}
	}

	return nil, syscall.Errno(syscall.ENOENT)
}

func (d Directory) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	directory, err := listDirectory("mine", d.Path)

	if err != nil {
		log.Println(err)
	}

	var dirents []fuse.Dirent

	for _, folder := range directory.Folders {
		dirents = append(dirents, fuse.Dirent{
			Name: folder.Name,
			Type: fuse.DT_Dir,
		})
	}

	for _, file := range directory.Files {
		dirents = append(dirents, fuse.Dirent{
			Name: file.Name,
			Type: fuse.DT_File,
		})
	}

	return dirents, nil
}

func (f File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 2
	a.Mode = 0o444
	a.Size = f.Size
	return nil
}

var fileCache = make(map[string][]byte)

func (f File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	data, ok := fileCache[f.PresignedUrl]
	if !ok {
		var err error
		data, err = getFile(f.PresignedUrl)
		if err != nil {
			return err
		}
		fileCache[f.PresignedUrl] = data
	}

	if req.Offset >= int64(len(data)) {
		return io.EOF
	}

	if req.Offset+int64(req.Size) > int64(len(data)) {
		resp.Data = data[req.Offset:]
	} else {
		resp.Data = data[req.Offset : req.Offset+int64(req.Size)]
	}

	return nil
}
