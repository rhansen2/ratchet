package util

import (
	"context"

	"github.com/rhansen2/ratchet/logger"
)

// KillPipelineIfErr is an error-checking helper.
func KillPipelineIfErr(err error, killChan chan error, ctx context.Context) {
	if err != nil {
		logger.Error(err.Error())
		select {
		case <-ctx.Done():
		case killChan <- err:
		}
	}
}
