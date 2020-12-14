package shard

import (
	"bufio"
	"io"
)

// WrappedWriter
type WrappedWriter struct {
	wrapped io.Writer
	bw      *bufio.Writer
	n       int64
}

// NewWrappedWriter returns a new WrappedWriter
func NewWrappedWriter(w io.Writer) (*WrappedWriter, error) {
	return &WrappedWriter{wrapped: w, bw: bufio.NewWriterSize(w, 1024*1024)}, nil
}

// Write writes a new block
func (w *WrappedWriter) Write(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	_, err := w.bw.Write(data)
	if err != nil {
		return err
	}

	n := len(data)
	// Increment file position pointer
	w.n += int64(n)

	return nil
}

func (w *WrappedWriter) WriteString(s string) error {
	if len(s) == 0 {
		return nil
	}

	_, err := w.bw.WriteString(s)
	if err != nil {
		return err
	}

	n := len(s)
	// Increment file position pointer
	w.n += int64(n)

	return nil
}

func (w *WrappedWriter) Flush() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}

	return w.sync()
}

func (w *WrappedWriter) sync() error {
	// sync is a minimal interface to make sure we can sync the wrapped
	// value. we use a minimal interface to be as robust as possible for
	// syncing these files.
	type sync interface {
		Sync() error
	}

	if f, ok := w.wrapped.(sync); ok {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (w *WrappedWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}

	if c, ok := w.wrapped.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (w *WrappedWriter) Size() uint32 {
	return uint32(w.n)
}
