package cmd

import (
	"fmt"
	"io"

	"github.com/zrepl/zrepl/zfs"
)

type DatasetMapping interface {
	Map(source *zfs.DatasetPath) (target *zfs.DatasetPath, err error)
}

type FilesystemRequest struct {
	Roots []string // may be nil, indicating interest in all filesystems
}

type FilesystemVersionsRequest struct {
	Filesystem *zfs.DatasetPath
}

type InitialTransferRequest struct {
	Filesystem        *zfs.DatasetPath
	FilesystemVersion zfs.FilesystemVersion
}

type IncrementalTransferRequest struct {
	Filesystem *zfs.DatasetPath
	From       zfs.FilesystemVersion
	To         zfs.FilesystemVersion
}

type Handler struct {
	Logger          Logger
	PullACL         zfs.DatasetFilter
	SinkMappingFunc func(clientIdentity string) (mapping DatasetMapping, err error)
}

func (h Handler) HandleFilesystemRequest(r *FilesystemRequest, roots *[]*zfs.DatasetPath) (err error) {

	h.Logger.Printf("handling fsr: %#v", r)

	h.Logger.Printf("using PullACL: %#v", h.PullACL)

	allowed, err := zfs.ZFSListMapping(h.PullACL)
	if err != nil {
		h.Logger.Printf("handle fsr err: %v\n", err)
		return
	}

	h.Logger.Printf("returning: %#v", allowed)
	*roots = allowed
	return
}

func (h Handler) HandleFilesystemVersionsRequest(r *FilesystemVersionsRequest, versions *[]zfs.FilesystemVersion) (err error) {

	h.Logger.Printf("handling filesystem versions request: %#v", r)

	// allowed to request that?
	if h.pullACLCheck(r.Filesystem); err != nil {
		return
	}

	// find our versions
	vs, err := zfs.ZFSListFilesystemVersions(r.Filesystem, nil)
	if err != nil {
		h.Logger.Printf("our versions error: %#v\n", err)
		return
	}

	h.Logger.Printf("our versions: %#v\n", vs)

	*versions = vs
	return

}

func (h Handler) HandleInitialTransferRequest(r *InitialTransferRequest, stream *io.Reader) (err error) {

	h.Logger.Printf("handling initial transfer request: %#v", r)
	if err = h.pullACLCheck(r.Filesystem); err != nil {
		return
	}

	h.Logger.Printf("invoking zfs send")

	s, err := zfs.ZFSSend(r.Filesystem, &r.FilesystemVersion, nil)
	if err != nil {
		h.Logger.Printf("error sending filesystem: %#v", err)
	}
	*stream = s

	return

}

func (h Handler) HandleIncrementalTransferRequest(r *IncrementalTransferRequest, stream *io.Reader) (err error) {

	h.Logger.Printf("handling incremental transfer request: %#v", r)
	if err = h.pullACLCheck(r.Filesystem); err != nil {
		return
	}

	h.Logger.Printf("invoking zfs send")

	s, err := zfs.ZFSSend(r.Filesystem, &r.From, &r.To)
	if err != nil {
		h.Logger.Printf("error sending filesystem: %#v", err)
	}

	*stream = s
	return

}

func (h Handler) pullACLCheck(p *zfs.DatasetPath) (err error) {
	var allowed bool
	allowed, err = h.PullACL.Filter(p)
	if err != nil {
		err = fmt.Errorf("error evaluating ACL: %s", err)
		h.Logger.Printf(err.Error())
		return
	}
	if !allowed {
		err = fmt.Errorf("ACL prohibits access to %s", p.ToString())
		h.Logger.Printf(err.Error())
		return
	}
	return
}
