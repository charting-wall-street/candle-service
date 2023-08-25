package database

import (
	"errors"
	"fmt"
	"github.com/godoji/candlestick"
	"kio/internal/store"
	"log"
	"strconv"
	"time"
)

func FetchCandles(symbol string, block int64, interval int64, useCache bool) (*candlestick.CandleSet, error) {

	// Retrieve symbol info
	symbolInfo := store.AssetInfo(symbol)
	if symbolInfo == nil {
		return nil, errors.New("symbol not found")
	}

	// Retrieve exchange info to get primitive intervals
	exchangeInfo := store.ExchangeInfo(symbolInfo.Identifier.Exchange)
	if exchangeInfo == nil {
		return nil, errors.New("exchange not found")
	}

	// Check cache for any existing versions
	key := getCacheKey(symbol, block, interval)
	if useCache {
		v, ok := candleSetCache.Get(key)
		if ok {
			return v.(*candlestick.CandleSet), nil
		}
	}

	// Return primitive data right from the source, if it is available
	smallestResolution := int64(-1)
	for _, candidate := range exchangeInfo.Resolution {
		if candidate == interval {
			return primitiveSet(symbol, block, interval)
		}
		if smallestResolution == -1 || candidate < smallestResolution {
			smallestResolution = candidate
		}
	}

	if interval < smallestResolution {
		return nil, nil
	}

	// Check onboard date
	onBoardDate, err := store.OnBoardDate(symbol)
	if err != nil {
		return nil, err
	}

	// Return nothing if the data requested is before on board date
	lastBlockCandle := candlestick.BlockToUnix(block+1, interval) - interval
	if lastBlockCandle < onBoardDate {
		return nil, nil
	}

	// Retrieve sub interval from which to create parent set
	subInterval, ok := candlestick.IntervalMap[interval]
	if !ok {
		errMsg := fmt.Sprintf("invalid interval requested %d", interval)
		log.Println(errMsg)
		return nil, errors.New(errMsg)
	}
	subNumber := interval / subInterval
	startTime := candlestick.BlockToUnix(block, interval)
	now := time.Now().UTC().Unix()

	// Fetch sub candles
	candles := make([]candlestick.Candle, 5000)
	lastTime := startTime - interval
	for i := 0; i < len(candles); i++ {
		lastTime += interval
		candles[i].Missing = true
		candles[i].Time = lastTime
	}

	// Keep track of set completion by looking at children
	isComplete := true

	// Iterate number of sub blocks
	for i := int64(0); i < subNumber; i++ {

		// Fetch sub block
		b := candlestick.UnixToBlock(startTime, subInterval) + i
		s, err := FetchCandles(symbol, b, subInterval, useCache)
		if err != nil {
			return nil, err
		}

		// Handle empty block as incomplete data
		if s == nil {
			isComplete = false
			continue
		}

		isComplete = isComplete && s.IsComplete()

		for j := range s.Candles {
			src := &s.Candles[j]
			index := (src.Time - startTime) / interval
			dst := &candles[index]
			mergeCandles(src, dst)
		}
	}

	data := &candlestick.CandleSet{
		Candles: candles,
		Meta: candlestick.DataSetMeta{
			UID:        symbol + ":" + strconv.FormatInt(interval, 10) + ":" + strconv.FormatInt(block, 10),
			Block:      block,
			Complete:   isComplete,
			LastUpdate: now,
			Symbol:     symbol,
			Interval:   interval,
		},
	}

	if !useCache {
		return data, nil
	}

	if !isComplete {
		candleSetCache.SetWithTTL(key, data, candleSetCost, 10*time.Second)
	} else {
		candleSetCache.Set(key, data, candleSetCost)
	}

	return data, nil
}
