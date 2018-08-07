package buildtsi

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"go.uber.org/zap"
)

func IndexShard(sfile *tsdb.SeriesFile, dataDir, walDir string, maxLogFileSize int64, batchSize int, log *zap.Logger, verboseLogging bool) error {
	log.Info("Rebuilding shard")

	// Check if shard already has a TSI index.
	indexPath := filepath.Join(dataDir, "index")
	log.Info("Checking index path", zap.String("path", indexPath))
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		log.Info("tsi1 index already exists, skipping", zap.String("path", indexPath))
		return nil
	}

	log.Info("Opening shard")

	// Find shard files.
	tsmPaths, err := collectTSMFiles(dataDir)
	if err != nil {
		return err
	}
	walPaths, err := collectWALFiles(walDir)
	if err != nil {
		return err
	}

	// Remove temporary index files if this is being re-run.
	tmpPath := filepath.Join(dataDir, ".index")
	log.Info("Cleaning up partial index from previous run, if any")
	if err := os.RemoveAll(tmpPath); err != nil {
		return err
	}

	// Open TSI index in temporary path.
	tsiIndex := tsi1.NewIndex(sfile, "",
		tsi1.WithPath(tmpPath),
		tsi1.WithMaximumLogFileSize(maxLogFileSize),
		tsi1.DisableFsync(),
		// Each new series entry in a log file is ~12 bytes so this should
		// roughly equate to one flush to the file for every batch.
		tsi1.WithLogFileBufferSize(12*batchSize),
	)

	tsiIndex.WithLogger(log)

	log.Info("Opening tsi index in temporary location", zap.String("path", tmpPath))
	if err := tsiIndex.Open(); err != nil {
		return err
	}
	defer tsiIndex.Close()

	// Write out tsm1 files.
	log.Info("Iterating over tsm files")
	for _, path := range tsmPaths {
		log.Info("Processing tsm file", zap.String("path", path))
		if err := IndexTSMFile(tsiIndex, path, batchSize, log, verboseLogging); err != nil {
			return err
		}
	}

	// Write out wal files.
	log.Info("Building cache from wal files")
	cache := tsm1.NewCache(tsdb.DefaultCacheMaxMemorySize)
	loader := tsm1.NewCacheLoader(walPaths)
	loader.WithLogger(log)
	if err := loader.Load(cache); err != nil {
		return err
	}

	log.Info("Iterating over cache")
	keysBatch := make([][]byte, 0, batchSize)
	namesBatch := make([][]byte, 0, batchSize)
	tagsBatch := make([]models.Tags, 0, batchSize)

	for _, key := range cache.Keys() {
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		name, tags := models.ParseKeyBytes(seriesKey)

		if verboseLogging {
			log.Info("Series", zap.String("name", string(name)), zap.String("tags", tags.String()))
		}

		keysBatch = append(keysBatch, seriesKey)
		namesBatch = append(namesBatch, name)
		tagsBatch = append(tagsBatch, tags)

		// Flush batch?
		if len(keysBatch) == batchSize {
			if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			keysBatch = keysBatch[:0]
			namesBatch = namesBatch[:0]
			tagsBatch = tagsBatch[:0]
		}
	}

	// Flush any remaining series in the batches
	if len(keysBatch) > 0 {
		if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch); err != nil {
			return fmt.Errorf("problem creating series: (%s)", err)
		}
		keysBatch = nil
		namesBatch = nil
		tagsBatch = nil
	}

	// Attempt to compact the index & wait for all compactions to complete.
	log.Info("compacting index")
	tsiIndex.Compact()
	tsiIndex.Wait()

	// Close TSI index.
	log.Info("Closing tsi index")
	if err := tsiIndex.Close(); err != nil {
		return err
	}

	// Rename TSI to standard path.
	log.Info("Moving tsi to permanent location")
	return os.Rename(tmpPath, indexPath)
}

func IndexTSMFile(index *tsi1.Index, path string, batchSize int, log *zap.Logger, verboseLogging bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := tsm1.NewTSMReader(f)
	if err != nil {
		log.Warn("Unable to read, skipping", zap.String("path", path), zap.Error(err))
		return nil
	}
	defer r.Close()

	keysBatch := make([][]byte, 0, batchSize)
	namesBatch := make([][]byte, 0, batchSize)
	tagsBatch := make([]models.Tags, batchSize)
	var ti int
	for i := 0; i < r.KeyCount(); i++ {
		key, _ := r.KeyAt(i)
		seriesKey, _ := tsm1.SeriesAndFieldFromCompositeKey(key)
		var name []byte
		name, tagsBatch[ti] = models.ParseKeyBytesWithTags(seriesKey, tagsBatch[ti])

		if verboseLogging {
			log.Info("Series", zap.String("name", string(name)), zap.String("tags", tagsBatch[ti].String()))
		}

		keysBatch = append(keysBatch, seriesKey)
		namesBatch = append(namesBatch, name)
		ti++

		// Flush batch?
		if len(keysBatch) == batchSize {
			if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch[:ti]); err != nil {
				return fmt.Errorf("problem creating series: (%s)", err)
			}
			keysBatch = keysBatch[:0]
			namesBatch = namesBatch[:0]
			ti = 0 // Reset tags.
		}
	}

	// Flush any remaining series in the batches
	if len(keysBatch) > 0 {
		if err := index.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch[:ti]); err != nil {
			return fmt.Errorf("problem creating series: (%s)", err)
		}
	}
	return nil
}

func collectTSMFiles(path string) ([]string, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != "."+tsm1.TSMFileExtension {
			continue
		}
		paths = append(paths, filepath.Join(path, fi.Name()))
	}
	return paths, nil
}

func collectWALFiles(path string) ([]string, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != "."+tsm1.WALFileExtension {
			continue
		}
		paths = append(paths, filepath.Join(path, fi.Name()))
	}
	return paths, nil
}
