package shard

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	DefaultFileSize      = uint32(1 * 1024 * 1024) // 1MB
	DefaultFileExtension = "shd"                   // 默认文件扩展名
	TmpFileExtension     = "tmp"                   // 临时文件扩展名
	// ErrMaxBlocksExceeded = fmt.Errorf("max blocks exceeded")
)

type Writer struct {
	ww    *WrappedWriter
	id    string
	path  string
	ext   string
	fsize uint32
	files []string
	seq   int
	err   error
}

type option func(w *Writer)

// Sequence specifies the starting sequence number of the files.
func Sequence(seq int) option {
	return func(w *Writer) {
		w.seq = seq
	}
}

// FileSize 分片文件大小
func FileSize(size uint32) option {
	return func(w *Writer) {
		w.fsize = size
	}
}

// Extension 文件扩展名
func Extension(ext string) option {
	return func(w *Writer) {
		w.ext = ext
	}
}

// Path 生成文件路径
func Path(path string) option {
	return func(w *Writer) {
		w.path = path
	}
}

func NewWriter(id string, opts ...option) *Writer {
	w := &Writer{id: id, seq: 1, fsize: DefaultFileSize, ext: DefaultFileExtension}

	for _, opt := range opts {
		opt(w)
	}

	w.next()

	return w
}

func (w *Writer) Write(data []byte) {
	if w.err != nil {
		return
	}

	if w.ww.Size() > w.fsize {
		w.close()
		w.next()
	}

	if err := w.ww.Write(data); err != nil {
		w.err = err
	}
}

func (w *Writer) WriteString(s string) {
	if w.err != nil {
		return
	}

	if w.ww.Size() > w.fsize {
		w.close()
		w.next()
	}

	if err := w.ww.WriteString(s); err != nil {
		w.err = err
	}
}

// Close closes the writer.
func (w *Writer) Close() {
	if w.ww != nil {
		w.close()
	}
}

// ShardID returns the shard id of the writer.
func (w *Writer) ShardID() string { return w.id }

func (w *Writer) Err() error { return w.err }

// Files returns the full paths of all the files written by the Writer.
func (w *Writer) Files() []string { return w.files }

func (w *Writer) next() {
	fileName := filepath.Join(w.path, w.id, fmt.Sprintf("%09d.%s", w.seq, w.ext))
	w.files = append(w.files, fileName)
	w.seq++

	os.MkdirAll(filepath.Dir(fileName), os.ModePerm)
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		w.err = err
		return
	}

	// Create the writer for the new file.
	w.ww, err = NewWrappedWriter(fd)
	if err != nil {
		w.err = err
		return
	}
}

func (w *Writer) close() {
	el := NewErrorList()
	if err := w.ww.Close(); err != nil {
		el.Add(err)
	}

	err := el.Err()
	if err != nil {
		w.err = err
	}

	w.ww = nil
}
