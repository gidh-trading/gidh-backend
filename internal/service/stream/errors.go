package stream

import "errors"

// ErrBacktestFinished indicates the backtest has completed reading all data
var ErrBacktestFinished = errors.New("backtest finished")
