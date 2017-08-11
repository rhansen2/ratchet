package processors

import (
	"context"
	"encoding/csv"
	"os"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/util"
)

// CSVReader reads csv content from a file
type CSVReader struct {
	filename string
}

// NewCSVReader creates a CSVReader that will read the file
// as csv data and send it line by line
func NewCSVReader(filename string) *CSVReader {
	return &CSVReader{filename: filename}
}

func (c *CSVReader) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	f, err := os.Open(c.filename)
	util.KillPipelineIfErr(err, killChan, ctx)
	reader := csv.NewReader(f)
	csvs, err := reader.ReadAll()
	util.KillPipelineIfErr(err, killChan, ctx)
	if len(csvs) < 2 {
		return
	}
	res := make([]map[string]interface{}, len(csvs)-1)
	headers := csvs[0]
	for i := 1; i < len(csvs); i++ {
		currObj := make(map[string]interface{})
		for j, header := range headers {
			currObj[header] = csvs[i][j]
		}
		res[i-1] = currObj
	}
	jd, err := data.NewJSON(res)
	util.KillPipelineIfErr(err, killChan, ctx)
	select {
	case outputChan <- jd:
	case <-ctx.Done():
	}
}

// Finish - see interface for documentation.
func (c *CSVReader) Finish(outputChan chan data.JSON, killChan chan error, ctx context.Context) {
}

func (c *CSVReader) String() string {
	return "CSVReader"
}
