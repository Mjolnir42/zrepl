package cmd

import (
	mapstructure "github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/rpc"
	"github.com/zrepl/zrepl/util"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type SourceJob struct {
	Name           string
	Serve          AuthenticatedChannelListenerFactory
	Datasets       *DatasetMapFilter
	SnapshotFilter *PrefixSnapshotFilter
	Interval       time.Duration
	Prune          PrunePolicy
	Debug          JobDebugSettings
}

func parseSourceJob(name string, i map[string]interface{}) (j *SourceJob, err error) {

	var asMap struct {
		Serve          map[string]interface{}
		Datasets       map[string]string
		SnapshotPrefix string `mapstructure:"snapshot_prefix"`
		Interval       string
		Prune          map[string]interface{}
		Debug          map[string]interface{}
	}

	if err = mapstructure.Decode(i, &asMap); err != nil {
		err = errors.Wrap(err, "mapstructure error")
		return nil, err
	}

	j = &SourceJob{Name: name}

	if j.Serve, err = parseAuthenticatedChannelListenerFactory(asMap.Serve); err != nil {
		return
	}

	if j.Datasets, err = parseDatasetMapFilter(asMap.Datasets, true); err != nil {
		return
	}

	if j.SnapshotFilter, err = parsePrefixSnapshotFilter(asMap.SnapshotPrefix); err != nil {
		return
	}

	if j.Interval, err = time.ParseDuration(asMap.Interval); err != nil {
		err = errors.Wrap(err, "cannot parse 'interval'")
		return
	}

	if j.Prune, err = parsePrunePolicy(asMap.Prune); err != nil {
		err = errors.Wrap(err, "cannot parse 'prune'")
		return
	}

	if err = mapstructure.Decode(asMap.Debug, &j.Debug); err != nil {
		err = errors.Wrap(err, "cannot parse 'debug'")
		return
	}

	return
}

func (j *SourceJob) JobName() string {
	return j.Name
}

func (j *SourceJob) JobDo(log Logger) (err error) {

	listener, err := j.Serve.Listen()
	if err != nil {
		return err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	rwcChan := make(chan io.ReadWriteCloser)

	// Serve connections until interrupted or error
outer:
	for {

		go func() {
			rwc, err := listener.Accept()
			if err != nil {
				log.Printf("error accepting connection: %s", err)
				close(rwcChan)
				return
			}
			rwcChan <- rwc
		}()

		select {

		case rwc, notClosed := <-rwcChan:

			if !notClosed {
				break outer // closed because of accept error
			}

			rwc, err := util.NewReadWriteCloserLogger(rwc, j.Debug.Conn.ReadDump, j.Debug.Conn.WriteDump)
			if err != nil {
				panic(err)
			}

			// construct connection handler
			handler := Handler{
				Logger:  log,
				PullACL: j.Datasets,
			}

			// handle connection
			rpcServer := rpc.NewServer(rwc)
			if j.Debug.RPC.Log {
				rpclog := util.NewPrefixLogger(log, "rpc")
				rpcServer.SetLogger(rpclog, true)
			}
			registerEndpoints(rpcServer, handler)
			if err = rpcServer.Serve(); err != nil {
				log.Printf("error serving connection: %s", err)
			}
			rwc.Close()

		case sig := <-sigChan:

			log.Printf("%s received", sig)
			break outer

		}

	}

	signal.Stop(sigChan)
	close(sigChan)

	log.Printf("closing listener")
	err = listener.Close()
	if err != nil {
		log.Printf("error closing listener: %s", err)
	}

	return nil

}
