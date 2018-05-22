package ratchet

import (
	"context"
	"fmt"
	"sync"

	"github.com/rhansen2/ratchet/data"
	"github.com/rhansen2/ratchet/logger"
	"github.com/rhansen2/ratchet/util"
)

// StartSignal is what's sent to a starting DataProcessor
// to kick off execution. Typically this value will be ignored.
var StartSignal = "GO"

// Pipeline is the main construct used for running a series of stages within a data pipeline.
type Pipeline struct {
	layout       *PipelineLayout
	Name         string // Name is simply for display purpsoses in log output.
	BufferLength int    // Set to control channel buffering, default is 8.
	PrintData    bool   // Set to true to log full data payloads (only in Debug logging mode).
	timer        *util.Timer
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	onComplete   func()
}

// NewPipeline creates a new pipeline ready to run the given DataProcessors.
// For more complex use-cases, see NewBranchingPipeline.
func NewPipeline(ctx context.Context, onComplete func(), processors ...DataProcessor) *Pipeline {
	p := &Pipeline{Name: "Pipeline"}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.onComplete = onComplete
	stages := make([]*PipelineStage, len(processors))
	for i, p := range processors {
		dp := Do(p)
		if i < len(processors)-1 {
			dp.Outputs(processors[i+1])
		}
		stages[i] = NewPipelineStage([]*dataProcessor{dp}...)
	}
	p.layout, _ = NewPipelineLayout(stages...)
	return p
}

// NewBranchingPipeline creates a new pipeline ready to run the
// given PipelineLayout, which can accommodate branching/merging
// between stages each containing variable number of DataProcessors.
// See the ratchet package documentation for code examples and diagrams.
func NewBranchingPipeline(ctx context.Context, onComplete func(), layout *PipelineLayout) *Pipeline {
	ctx, cancel := context.WithCancel(ctx)
	p := &Pipeline{layout: layout, Name: "Pipeline", ctx: ctx, cancel: cancel, onComplete: onComplete}
	return p
}

// In order to support the branching PipelineLayout creation syntax, the
// dataProcessor.outputs are "DataProcessor" interface types, and not the "dataProcessor"
// wrapper types. This function loops through the layout and matches the
// interface to wrapper objects and returns them.
func (p *Pipeline) dataProcessorOutputs(dp *dataProcessor) []*dataProcessor {
	dpouts := make([]*dataProcessor, len(dp.outputs))
	for i := range dp.outputs {
		for _, stage := range p.layout.stages {
			for j := range stage.processors {
				if dp.outputs[i] == stage.processors[j].DataProcessor {
					dpouts[i] = stage.processors[j]
				}
			}
		}
	}
	return dpouts
}

// At this point in pipeline initialization, every dataProcessor has an input
// and output channel, but there is nothing connecting them together. In order
// to support branching and merging between stages (as defined by each
// dataProcessor's outputs), we set up some intermediary channels that will
// manage copying and passing data between stages, as well as properly closing
// channels when all data is received.
func (p *Pipeline) connectStages() {
	logger.Debug(p.Name, ": connecting stages")
	// First, setup the bridgeing channels & brancher/merger's to aid in
	// managing channel communication between processors.
	for _, stage := range p.layout.stages {
		for _, from := range stage.processors {
			if from.outputs != nil {
				from.branchOutChans = []chan data.JSON{}
				for _, to := range p.dataProcessorOutputs(from) {
					if to.mergeInChans == nil {
						to.mergeInChans = []chan data.JSON{}
					}
					c := p.initDataChan()
					from.branchOutChans = append(from.branchOutChans, c)
					to.mergeInChans = append(to.mergeInChans, c)
				}
			}
		}
	}
	// Loop through again and setup goroutines to handle data management
	// between the branchers and mergers
	for _, stage := range p.layout.stages {
		for _, dp := range stage.processors {
			dp.ctx = p.ctx
			if dp.branchOutChans != nil {
				dp.branchOut()
			}
			if dp.mergeInChans != nil {
				dp.mergeIn()
			}
		}
	}
}

func (p *Pipeline) runStages(killChan chan error) {
	for n, stage := range p.layout.stages {
		for _, dp := range stage.processors {
			numWorkers := 1
			if dp.concurrency > 1 {
				numWorkers = dp.concurrency
			}
			p.wg.Add(numWorkers)
			var concurrencyWg sync.WaitGroup
			concurrencyWg.Add(numWorkers)
			for i := 0; i < numWorkers; i++ {
				// Each DataProcessor runs in a separate gorountine.
				go func(n int, dp *dataProcessor, i int) {
					defer p.wg.Done()
					defer concurrencyWg.Done()
					// This is where the main DataProcessor interface
					// functions are called.
					logger.Info(p.Name, "- stage", n+1, dp, "waiting to receive data")
				processData:
					for {
						select {
						case d, ok := <-dp.inputChan:
							if !ok {
								break processData
							}
							logger.Info(p.Name, "- stage", n+1, dp, "received data")
							if p.PrintData {
								logger.Debug(p.Name, "- stage", n+1, dp, "data =", string(d))
							}
							dp.recordDataReceived(d)
							dp.processData(d, killChan)
						case <-p.ctx.Done():
							return
						}
					}
					logger.Info(p.Name, "- stage", n+1, dp, "input closed, calling Finish")
					dp.Finish(dp.outputChan, killChan, p.ctx)
				}(n, dp, i)
			}
			go func(dp *dataProcessor, n int) {
				concurrencyWg.Wait()
				if dp.outputChan != nil {
					close(dp.outputChan)
				}
			}(dp, n)
		}
	}
}

// Run finalizes the channel connections between PipelineStages
// and kicks off execution.
// Run will return a killChan that should be waited on so your calling function doesn't
// return prematurely. Any stage of the pipeline can send to the killChan to halt
// execution. Your calling function should check if the sent value is an error or nil to know if
// execution was a failure or a success (nil being the success value).
func (p *Pipeline) Run() (killChan chan error) {
	p.timer = util.StartTimer()
	killChan = make(chan error)

	innerKillChan := make(chan error)
	p.connectStages()
	p.runStages(innerKillChan)

	for _, dp := range p.layout.stages[0].processors {
		logger.Debug(p.Name, ": sending", StartSignal, "to", dp)
		dp.inputChan <- data.JSON(StartSignal)
		dp.Finish(dp.outputChan, innerKillChan, p.ctx)
		close(dp.inputChan)
	}

	// After all the stages are running, send the StartSignal
	// to the initial stage processors to kick off execution, and
	// then wait until all the processing goroutines are done to
	// signal successful pipeline completion.
	go func() {
		p.wg.Wait()
		p.timer.Stop()
	}()

	go func() {
		defer func() {
			if p.onComplete != nil {
				p.onComplete()
			}
			if p.cancel != nil {
				p.cancel()
			}
		}()
		for {
			select {
			case err := <-innerKillChan:
				p.cancel()
				killChan <- err
				close(killChan)
				return
			case <-p.ctx.Done():
				killChan <- p.ctx.Err()
				close(killChan)
				return
			}
		}
	}()
	return killChan
}

func (p *Pipeline) Cleanup() {
	if p.onComplete != nil {
		p.onComplete()
	}
}

func (p *Pipeline) initDataChans(length int) []chan data.JSON {
	cs := make([]chan data.JSON, length)
	for i := range cs {
		cs[i] = p.initDataChan()
	}
	return cs
}
func (p *Pipeline) initDataChan() chan data.JSON {
	return make(chan data.JSON, p.BufferLength)
}

// func (p *Pipeline) String() string {
// 	// Print an overview of the pipeline
// 	stageNames := []string{}
// 	for _, s := range p.layout.stages {
// 		stageNames = append(stageNames, fmt.Sprintf("%v", s))
// 	}
// 	return p.Name + ": " + strings.Join(stageNames, " -> "))
// }

// Stats returns a string (formatted for output display) listing the stats
// gathered for each stage executed.
func (p *Pipeline) Stats() string {
	o := fmt.Sprintf("%s: %s\r\n", p.Name, p.timer)
	for n, stage := range p.layout.stages {
		o += fmt.Sprintf("Stage %d)\r\n", n+1)
		for _, dp := range stage.processors {
			o += fmt.Sprintf("  * %v\r\n", dp)
			dp.executionStat.calculate()
			o += fmt.Sprintf("     - Total/Avg Execution Time = %f/%fs\r\n", dp.totalExecutionTime, dp.avgExecutionTime)
			o += fmt.Sprintf("     - Payloads Sent/Received = %d/%d\r\n", dp.dataSentCounter, dp.dataReceivedCounter)
			o += fmt.Sprintf("     - Total/Avg Bytes Sent = %d/%d\r\n", dp.totalBytesSent, dp.avgBytesSent)
			o += fmt.Sprintf("     - Total/Avg Bytes Received = %d/%d\r\n", dp.totalBytesReceived, dp.avgBytesReceived)
		}
	}
	return o
}
