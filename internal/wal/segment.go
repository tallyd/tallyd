package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// segment is one file in the WAL's append-only sequence. Only the active
// (most recent) segment ever has an open file handle for appending; older
// segments are tracked just enough to know when every event record inside
// them has been resolved and the file can be deleted.
type segment struct {
	seq  uint64
	path string
	f    *os.File
	size int64

	// refCount is the number of unresolved event entries whose original
	// event record was written to this segment. It reaches zero once every
	// event first written here has been acked or dead-lettered by every
	// target provider.
	refCount int
}

func segmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.wal", seq))
}

func createSegment(dir string, seq uint64) (*segment, error) {
	path := segmentPath(dir, seq)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	return &segment{seq: seq, path: path, f: f}, nil
}

func openSegmentForAppend(dir string, seq uint64) (*segment, error) {
	path := segmentPath(dir, seq)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &segment{seq: seq, path: path, f: f, size: info.Size()}, nil
}

// listSegmentSeqs returns the sequence numbers of every segment file found
// in dir, sorted oldest first. Missing dir is not an error (fresh WAL).
func listSegmentSeqs(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var seqs []uint64
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".wal" {
			continue
		}
		var seq uint64
		if _, err := fmt.Sscanf(e.Name(), "%020d.wal", &seq); err != nil {
			continue
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

func (s *segment) append(data []byte) error {
	n, err := s.f.Write(data)
	s.size += int64(n)
	return err
}

func (s *segment) sync() error {
	return s.f.Sync()
}

func (s *segment) close() error {
	return s.f.Close()
}
