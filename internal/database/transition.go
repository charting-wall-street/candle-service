package database

import (
	"github.com/godoji/candlestick"
	"strconv"
	"time"
)

func FetchTransition(symbol string, block int64, interval int64, resolution int64) (*candlestick.CandleSet, error) {

	// Skip candling when interval is 1 minute
	if interval == resolution {
		return primitiveSet(symbol, block, interval)
	}

	// fetch last minute candles to generate first candle on
	lastBlock, err := primitiveSet(symbol, block-1, resolution)
	if err != nil {
		return nil, err
	}

	// fetch current minute candles
	currBlock, err := primitiveSet(symbol, block, resolution)
	if err != nil {
		return nil, err
	}

	// check if current set exists
	if currBlock == nil {
		return nil, nil
	}

	// generate first part of first candle
	blockStartTime := currBlock.UnixFirst()
	firstCandleTime := blockStartTime / interval * interval
	lastIndex := blockStartTime / interval
	lastCandle := candlestick.Candle{
		Time:    firstCandleTime,
		Missing: true,
	}
	if lastBlock != nil {
		for i := lastBlock.Index(firstCandleTime); i < 5000; i++ {
			c := lastBlock.AtIndex(i)
			mergeCandles(c, &lastCandle)
			lastCandle.Time = c.Time
		}
	}

	candles := make([]candlestick.Candle, 5000)
	for i := range currBlock.Candles {
		c := &currBlock.Candles[i]
		newIndex := c.Time / interval
		if newIndex != lastIndex {
			lastIndex = newIndex
			lastCandle = candlestick.Candle{
				Time:    c.Time,
				Missing: true,
			}
		}
		mergeCandles(c, &lastCandle)
		lastCandle.Time = c.Time
		candles[i] = lastCandle
	}

	return &candlestick.CandleSet{
		Candles: candles,
		Meta: candlestick.DataSetMeta{
			UID:        symbol + ":" + strconv.FormatInt(interval, 10) + ":" + strconv.FormatInt(block, 10) + ":t",
			Block:      block,
			Complete:   currBlock.IsComplete(),
			LastUpdate: time.Now().UTC().Unix(),
			Symbol:     symbol,
			Interval:   resolution,
		},
	}, nil

}
