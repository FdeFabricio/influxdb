// Package buildtsi reads an in-memory index and exports it as a TSI index.
package buildtsi

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

const defaultBatchSize = 10000

// Command represents the program execution for "influx_inspect buildtsi".
type Command struct {
	Stderr  io.Writer
	Stdout  io.Writer
	Verbose bool
	Logger  *zap.Logger

	concurrency     int // Number of goroutines to dedicate to shard index building.
	databaseFilter  string
	retentionFilter string
	shardFilter     string
	maxLogFileSize  int64
	batchSize       int
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr:      os.Stderr,
		Stdout:      os.Stdout,
		Logger:      zap.NewNop(),
		batchSize:   defaultBatchSize,
		concurrency: runtime.GOMAXPROCS(0),
	}
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	fs := flag.NewFlagSet("buildtsi", flag.ExitOnError)
	dataDir := fs.String("datadir", "", "data directory")
	walDir := fs.String("waldir", "", "WAL directory")
	fs.IntVar(&cmd.concurrency, "concurrency", runtime.GOMAXPROCS(0), "Number of workers to dedicate to shard index building. Defaults to GOMAXPROCS")
	fs.StringVar(&cmd.databaseFilter, "database", "", "optional: database name")
	fs.StringVar(&cmd.retentionFilter, "retention", "", "optional: retention policy")
	fs.StringVar(&cmd.shardFilter, "shard", "", "optional: shard id")
	fs.Int64Var(&cmd.maxLogFileSize, "max-log-file-size", tsdb.DefaultMaxIndexLogFileSize, "optional: maximum log file size")
	fs.IntVar(&cmd.batchSize, "batch-size", defaultBatchSize, "optional: set the size of the batches we write to the index. Setting this can have adverse affects on performance and heap requirements")
	fs.BoolVar(&cmd.Verbose, "v", false, "verbose")
	fs.SetOutput(cmd.Stdout)
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() > 0 || *dataDir == "" || *walDir == "" {
		fs.Usage()
		return nil
	}
	cmd.Logger = logger.New(cmd.Stderr)

	return cmd.run(*dataDir, *walDir)
}

func (cmd *Command) run(dataDir, walDir string) error {
	// Verify the user actually wants to run as root.
	if isRoot() {
		fmt.Println("You are currently running as root. This will build your")
		fmt.Println("index files with root ownership and will be inaccessible")
		fmt.Println("if you run influxd as a non-root user. You should run")
		fmt.Println("buildtsi as the same user you are running influxd.")
		fmt.Print("Are you sure you want to continue? (y/N): ")
		var answer string
		if fmt.Scanln(&answer); !strings.HasPrefix(strings.TrimSpace(strings.ToLower(answer)), "y") {
			return fmt.Errorf("operation aborted")
		}
	}

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		name := fi.Name()
		if !fi.IsDir() {
			continue
		} else if cmd.databaseFilter != "" && name != cmd.databaseFilter {
			continue
		}

		if err := cmd.processDatabase(name, filepath.Join(dataDir, name), filepath.Join(walDir, name)); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) processDatabase(dbName, dataDir, walDir string) error {
	cmd.Logger.Info("Rebuilding database", zap.String("name", dbName))

	sfile := tsdb.NewSeriesFile(filepath.Join(dataDir, tsdb.SeriesFileDirectory))
	sfile.Logger = cmd.Logger
	if err := sfile.Open(); err != nil {
		return err
	}
	defer sfile.Close()

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		rpName := fi.Name()
		if !fi.IsDir() {
			continue
		} else if rpName == tsdb.SeriesFileDirectory {
			continue
		} else if cmd.retentionFilter != "" && rpName != cmd.retentionFilter {
			continue
		}

		if err := cmd.processRetentionPolicy(sfile, dbName, rpName, filepath.Join(dataDir, rpName), filepath.Join(walDir, rpName)); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *Command) processRetentionPolicy(sfile *tsdb.SeriesFile, dbName, rpName, dataDir, walDir string) error {
	cmd.Logger.Info("Rebuilding retention policy", logger.Database(dbName), logger.RetentionPolicy(rpName))

	fis, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return err
	}

	type shard struct {
		ID   uint64
		Path string
	}

	var shards []shard

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		} else if cmd.shardFilter != "" && fi.Name() != cmd.shardFilter {
			continue
		}

		shardID, err := strconv.ParseUint(fi.Name(), 10, 64)
		if err != nil {
			continue
		}

		shards = append(shards, shard{shardID, fi.Name()})
	}

	errC := make(chan error, len(shards))
	var maxi uint32 // index of maximum shard being worked on.
	for k := 0; k < cmd.concurrency; k++ {
		go func() {
			for {
				i := int(atomic.AddUint32(&maxi, 1) - 1) // Get next partition to work on.
				if i >= len(shards) {
					return // No more work.
				}

				id, name := shards[i].ID, shards[i].Path
				log := cmd.Logger.With(logger.Database(dbName), logger.RetentionPolicy(rpName), logger.Shard(id))
				errC <- IndexShard(sfile, filepath.Join(dataDir, name), filepath.Join(walDir, name), cmd.maxLogFileSize, cmd.batchSize, log, cmd.Verbose)
			}
		}()
	}

	// Check for error
	for i := 0; i < cap(errC); i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

func isRoot() bool {
	user, _ := user.Current()
	return user != nil && user.Username == "root"
}
