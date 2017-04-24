package processors

import (
	"context"
	"io"

	"github.com/jlaffaye/ftp"
	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/logger"
	"github.com/rhansen2/ratchet/util"
)

// FtpWriter type represents an ftp writter processor
type FtpWriter struct {
	ftpFilepath   string
	conn          *ftp.ServerConn
	fileWriter    *io.PipeWriter
	authenticated bool
	host          string
	username      string
	password      string
	path          string
}

// NewFtpWriter instantiates new instance of an ftp writer
func NewFtpWriter(host, username, password, path string) *FtpWriter {
	return &FtpWriter{authenticated: false, host: host, username: username, password: password, path: path}
}

// connect - opens a connection to the provided ftp host and then authenticates with the host with the username, password attributes
func (f *FtpWriter) connect(killChan chan error, ctx context.Context) {
	conn, err := ftp.Dial(f.host)
	if err != nil {
		util.KillPipelineIfErr(err, killChan, ctx)
	}

	lerr := conn.Login(f.username, f.password)
	if lerr != nil {
		util.KillPipelineIfErr(lerr, killChan, ctx)
	}

	r, w := io.Pipe()

	f.conn = conn
	go f.conn.Stor(f.path, r)
	f.fileWriter = w
	f.authenticated = true
}

// ProcessData writes data as is directly to the output file
func (f *FtpWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error, ctx context.Context) {
	logger.Debug("FTPWriter Process data:", string(d))
	if !f.authenticated {
		f.connect(killChan, ctx)
	}

	_, e := f.fileWriter.Write([]byte(d))
	if e != nil {
		util.KillPipelineIfErr(e, killChan, ctx)
	}
}

// Finish closes open references to the remote file and server
func (f *FtpWriter) Finish(outputChan chan data.JSON, killChan chan error) {
	if f.fileWriter != nil {
		f.fileWriter.Close()
	}
	if f.conn != nil {
		f.conn.Logout()
		f.conn.Quit()
	}
}

func (f *FtpWriter) String() string {
	return "FtpWriter"
}
