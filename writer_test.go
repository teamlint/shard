package shard

import (
	"bufio"
	"io"
	"os"
	"testing"
)

func TestWriter(t *testing.T) {
	w := NewWriter("dump", FileSize(5*1024*1024), Extension("sql"))

	fs, err := os.Open("./test.sql")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	r := bufio.NewReaderSize(fs, 1024*16)
	for {
		line, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				t.Fatal(err)
			}
		}
		// t.Logf("line = %s\n", line)
		line = append(line, "\n"...)
		w.Write(line)
	}
	defer w.Close()
	t.Logf("files = %v", w.Files())

}
