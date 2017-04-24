package processors

import (
	"context"
	"fmt"
	"io"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/logger"
	"github.com/rhansen2/ratchet/util"
)

// IoWriter wraps any io.Writer object.
// It can be used to write data out to a File, os.Stdout, or
// any other task that can be supported via io.Writer.
type IoWriter struct {
	Writer     io.Writer
	AddNewline bool
}

// NewIoWriter returns a new IoWriter wrapping the given io.Writer object
func NewIoWriter(writer io.Writer) *IoWriter {
	return &IoWriter{Writer: writer, AddNewline: false}
}

// ProcessData writes the data
func (w *IoWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	var bytesWritten int
	var err error
	if w.AddNewline {
		bytesWritten, err = fmt.Fprintln(w.Writer, string(d))
	} else {
		bytesWritten, err = w.Writer.Write(d)
	}
	util.KillPipelineIfErr(err, killChan, ctx)
	logger.Debug("IoWriter:", bytesWritten, "bytes written")
}

// Finish - see interface for documentation.
func (w *IoWriter) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {
}

func (w *IoWriter) String() string {
	return "IoWriter"
}
