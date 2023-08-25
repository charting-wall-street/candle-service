package database

import (
	"github.com/godoji/candlestick"
	"kio/internal/store"
	"math"
	"time"
)

func primitiveSet(symbol string, block int64, resolution int64) (*candlestick.CandleSet, error) {

	// try cache first
	key := getCacheKey(symbol, block, resolution)

	// check if block is in memory or on disk
	cbn := CacheBlockNumber(resolution)
	if cbn == block {

		result := RealTimeBlock(symbol, block)

		// make sure we got the correct block, otherwise check disk
		if result.BlockNumber() == block {
			candleSetCache.SetWithTTL(key, result, candleSetCost, 10*time.Second)
			return result, nil
		}

	}

	// check if block is from the future
	if cbn != math.MinInt64 && cbn < block {
		return nil, nil
	}

	// try fetch from disk, if it fails the block is nowhere to be found
	result, err := store.LoadFromDisk(symbol, block, resolution)
	if err != nil {
		return nil, err
	}

	info := store.AssetInfo(symbol)
	if result != nil && len(info.Splits) > 0 {
		for i := range result.Candles {
			for j := len(info.Splits) - 1; j >= 0; j-- {
				split := info.Splits[j]
				if result.Candles[i].Time >= split.Time {
					break
				}
				result.Candles[i].Open /= split.Ratio
				result.Candles[i].High /= split.Ratio
				result.Candles[i].Low /= split.Ratio
				result.Candles[i].Close /= split.Ratio
			}
		}
	}

	candleSetCache.Set(key, result, candleSetCost)

	return result, err
}

func mergeCandles(src *candlestick.Candle, dst *candlestick.Candle) {
	if src.Missing {
		return
	}
	if dst.Missing {
		dst.Missing = false
		dst.Open = src.Open
		dst.Low = src.Low
	}
	if src.High > dst.High {
		dst.High = src.High
	}
	if src.Low < dst.Low {
		dst.Low = src.Low
	}
	dst.Close = src.Close
	dst.Volume += src.Volume
	dst.TakerVolume += src.TakerVolume
	dst.NumberOfTrades += src.NumberOfTrades
}
