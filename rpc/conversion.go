package rpc

import (
	"github.com/jinzhu/copier"
	"github.com/zrepl/zrepl/zfs"
)

func ToZFSFilesystemVersion(a *FilesystemVersion) (b zfs.FilesystemVersion, err error) {
	err = copier.Copy(&b, a)
	return
}

func FromZFSFilesystemVersion(a *zfs.FilesystemVersion) (b *FilesystemVersion, err error) {
	b = &FilesystemVersion{}
	err = copier.Copy(b, a)
	return
}
