package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	Enc = binary.BigEndian
)

const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(file *os.File) (*store, error) {
	fi, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: file,
		buf:  bufio.NewWriter(file),
		size: size,
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err = binary.Write(s.buf, Enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	write, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, nil
	}
	write += lenWidth
	s.size += uint64(write)
	return uint64(write), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	bytes := make([]byte, Enc.Uint64(size))
	if _, err := s.File.ReadAt(bytes, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return bytes, nil
}

func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
