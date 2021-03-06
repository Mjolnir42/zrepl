package cmd

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/rpc"
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
	logger Logger
	dsf    zfs.DatasetFilter
	fsvf   zfs.FilesystemVersionFilter
}

func NewHandler(logger Logger, dsfilter zfs.DatasetFilter, snapfilter zfs.FilesystemVersionFilter) (h Handler) {
	return Handler{logger, dsfilter, snapfilter}
}

func registerEndpoints(server rpc.RPCServer, handler Handler) (err error) {
	err = server.RegisterEndpoint("FilesystemRequest", handler.HandleFilesystemRequest)
	if err != nil {
		panic(err)
	}
	err = server.RegisterEndpoint("FilesystemVersionsRequest", handler.HandleFilesystemVersionsRequest)
	if err != nil {
		panic(err)
	}
	err = server.RegisterEndpoint("InitialTransferRequest", handler.HandleInitialTransferRequest)
	if err != nil {
		panic(err)
	}
	err = server.RegisterEndpoint("IncrementalTransferRequest", handler.HandleIncrementalTransferRequest)
	if err != nil {
		panic(err)
	}
	return nil
}

func (h Handler) HandleFilesystemRequest(r *FilesystemRequest, roots *[]*zfs.DatasetPath) (err error) {

	h.logger.Printf("handling fsr: %#v", r)

	h.logger.Printf("using dsf: %#v", h.dsf)

	allowed, err := zfs.ZFSListMapping(h.dsf)
	if err != nil {
		h.logger.Printf("handle fsr err: %v\n", err)
		return
	}

	h.logger.Printf("returning: %#v", allowed)
	*roots = allowed
	return
}

func (h Handler) HandleFilesystemVersionsRequest(r *FilesystemVersionsRequest, versions *[]zfs.FilesystemVersion) (err error) {

	h.logger.Printf("handling filesystem versions request: %#v", r)

	// allowed to request that?
	if h.pullACLCheck(r.Filesystem, nil); err != nil {
		return
	}

	// find our versions
	vs, err := zfs.ZFSListFilesystemVersions(r.Filesystem, h.fsvf)
	if err != nil {
		h.logger.Printf("our versions error: %#v\n", err)
		return
	}

	h.logger.Printf("our versions: %#v\n", vs)

	*versions = vs
	return

}

func (h Handler) HandleInitialTransferRequest(r *InitialTransferRequest, stream *io.Reader) (err error) {

	h.logger.Printf("handling initial transfer request: %#v", r)
	if err = h.pullACLCheck(r.Filesystem, &r.FilesystemVersion); err != nil {
		return
	}

	h.logger.Printf("invoking zfs send")

	s, err := zfs.ZFSSend(r.Filesystem, &r.FilesystemVersion, nil)
	if err != nil {
		h.logger.Printf("error sending filesystem: %#v", err)
	}
	*stream = s

	return

}

func (h Handler) HandleIncrementalTransferRequest(r *IncrementalTransferRequest, stream *io.Reader) (err error) {

	h.logger.Printf("handling incremental transfer request: %#v", r)
	if err = h.pullACLCheck(r.Filesystem, &r.From); err != nil {
		return
	}
	if err = h.pullACLCheck(r.Filesystem, &r.To); err != nil {
		return
	}

	h.logger.Printf("invoking zfs send")

	s, err := zfs.ZFSSend(r.Filesystem, &r.From, &r.To)
	if err != nil {
		h.logger.Printf("error sending filesystem: %#v", err)
	}

	*stream = s
	return

}

func (h Handler) pullACLCheck(p *zfs.DatasetPath, v *zfs.FilesystemVersion) (err error) {
	var fsAllowed, vAllowed bool
	fsAllowed, err = h.dsf.Filter(p)
	if err != nil {
		err = fmt.Errorf("error evaluating ACL: %s", err)
		h.logger.Printf(err.Error())
		return
	}
	if !fsAllowed {
		err = fmt.Errorf("ACL prohibits access to %s", p.ToString())
		h.logger.Printf(err.Error())
		return
	}
	if v == nil {
		return
	}

	vAllowed, err = h.fsvf.Filter(*v)
	if err != nil {
		err = errors.Wrap(err, "error evaluating version filter")
		h.logger.Printf(err.Error())
		return
	}
	if !vAllowed {
		err = fmt.Errorf("ACL prohibits access to %s", v.ToAbsPath(p))
		h.logger.Printf(err.Error())
		return
	}
	return
}
