package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	file, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(file.Name())

	config := Config{}
	config.Segment.MaxIndexBytes = 1024
	i, err := newIndex(file, config)
	require.NoError(t, err)
	_, _, err = i.Read(-1)
	require.Error(t, err)
	require.Equal(t, file.Name(), i.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, entry := range entries {
		err := i.Write(entry.Off, entry.Pos)
		require.NoError(t, err)

		off, pos, err := i.Read(int64(entry.Off))
		require.NoError(t, err)
		require.Equal(t, entry.Off, off)
		require.Equal(t, entry.Pos, pos)
	}

	_, _, err = i.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = i.Close()

	file, _ = os.OpenFile(file.Name(), os.O_RDWR, 0600)
	i, err = newIndex(file, config)
	require.NoError(t, err)
	off, pos, err := i.Read(-1)
	require.NoError(t, err)
	require.Equal(t, entries[1].Off, off)
	require.Equal(t, entries[1].Pos, pos)

}
