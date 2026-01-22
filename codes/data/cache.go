package data

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"transfer-graph-evm/search"
)

// SaveMainGraph writes a MainGraph into a gob file on disk.
func SaveMainGraph(path string, mg search.MainGraph) error {
	if path == "" {
		return errors.New("path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create cache file: %w", err)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(mg); err != nil {
		return fmt.Errorf("encode main graph: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync cache file: %w", err)
	}
	return nil
}

// LoadMainGraph reads a MainGraph from a gob file.
func LoadMainGraph(path string) (search.MainGraph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open cache file: %w", err)
	}
	defer f.Close()

	dec := gob.NewDecoder(f)
	var mg search.MainGraph
	if err := dec.Decode(&mg); err != nil {
		return nil, fmt.Errorf("decode main graph: %w", err)
	}
	return mg, nil
}

// MainGraphCache stores a slice of MainGraph on disk for indexed access.
type MainGraphCache struct {
	dir string
}

// NewMainGraphCache prepares a cache directory for storing graphs.
func NewMainGraphCache(dir string) (*MainGraphCache, error) {
	if dir == "" {
		return nil, errors.New("dir is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	return &MainGraphCache{dir: dir}, nil
}

func (c *MainGraphCache) pathForIndex(idx int) (string, error) {
	if idx < 0 {
		return "", errors.New("index must be non-negative")
	}
	if c == nil {
		return "", errors.New("cache is nil")
	}
	if c.dir == "" {
		return "", errors.New("cache directory is empty")
	}
	return filepath.Join(c.dir, fmt.Sprintf("%06d.mgob", idx)), nil
}

// WriteAll replaces the cache contents with the provided graphs.
func (c *MainGraphCache) WriteAll(graphs []search.MainGraph) error {
	if c == nil {
		return errors.New("cache is nil")
	}
	if c.dir == "" {
		return errors.New("cache directory is empty")
	}

	if err := os.RemoveAll(c.dir); err != nil {
		return fmt.Errorf("clear cache dir: %w", err)
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return fmt.Errorf("recreate cache dir: %w", err)
	}

	for i, g := range graphs {
		path, err := c.pathForIndex(i)
		if err != nil {
			return err
		}
		if err := SaveMainGraph(path, g); err != nil {
			return fmt.Errorf("write main graph %d: %w", i, err)
		}
	}
	return nil
}

// Read loads the cached MainGraph at the given index.
func (c *MainGraphCache) Read(index int) (search.MainGraph, error) {
	path, err := c.pathForIndex(index)
	if err != nil {
		return nil, err
	}
	return LoadMainGraph(path)
}

// Free removes all cached files from disk.
func (c *MainGraphCache) Free() error {
	if c == nil || c.dir == "" {
		return nil
	}
	dir := c.dir
	c.dir = ""
	return os.RemoveAll(dir)
}
