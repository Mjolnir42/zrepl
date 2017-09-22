package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

// daemonCmd represents the daemon command
var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "start daemon",
	Run:   doDaemon,
}

func init() {
	RootCmd.AddCommand(daemonCmd)
}

type Job interface {
	JobName() string
	JobStart(ctxt context.Context)
}

func doDaemon(cmd *cobra.Command, args []string) {

	conf, err := ParseConfig(rootArgs.configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing config: %s", err)
		os.Exit(1)
	}

	log := conf.Global.logging.MakeLogrus()
	log.Debug("starting daemon")
	ctx := context.WithValue(context.Background(), contextKeyLog, log)
	ctx = context.WithValue(ctx, contextKeyLog, log)

	d := NewDaemon(conf)
	d.Loop(ctx)

}

type contextKey string

const (
	contextKeyLog contextKey = contextKey("log")
)

type Daemon struct {
	conf *Config
}

func NewDaemon(initialConf *Config) *Daemon {
	return &Daemon{initialConf}
}

func (d *Daemon) Loop(ctx context.Context) {

	log := ctx.Value(contextKeyLog).(Logger)

	ctx, cancel := context.WithCancel(ctx)

	sigChan := make(chan os.Signal, 1)
	finishs := make(chan Job)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("starting jobs from config")
	i := 0
	for _, job := range d.conf.Jobs {
		log.Printf("starting job %s", job.JobName())

		logger := log.WithField("job", job.JobName())
		i++
		jobCtx := context.WithValue(ctx, contextKeyLog, logger)
		go func(j Job) {
			j.JobStart(jobCtx)
			finishs <- j
		}(job)
	}

	finishCount := 0
outer:
	for {
		select {
		case j := <-finishs:
			log.Printf("job finished: %s", j.JobName())
			finishCount++
			if finishCount == len(d.conf.Jobs) {
				log.Printf("all jobs finished")
				break outer
			}

		case sig := <-sigChan:
			log.Printf("received signal: %s", sig)
			log.Printf("cancelling all jobs")
			cancel()
		}
	}

	signal.Stop(sigChan)

	log.Printf("exiting")

}
