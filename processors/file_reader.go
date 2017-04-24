package processors

import (
	"context"
	"io/ioutil"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
)

// FileReader opens and reads the contents of the given filename.
type FileReader struct {
	filename string
}

// NewFileReader returns a new FileReader that will read the entire contents
// of the given file path and send it at once. For buffered or line-by-line
// reading try using IoReader.
func NewFileReader(filename string) *FileReader {
	return &FileReader{filename: filename}
}

// ProcessData reads a file and sends its contents to outputChan
func (r *FileReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	d, err := ioutil.ReadFile(r.filename)
	util.KillPipelineIfErr(err, killChan, ctx)
	outputChan <- d
}

// Finish - see interface for documentation.
func (r *FileReader) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {
}

func (r *FileReader) String() string {
	return "FileReader"
}
