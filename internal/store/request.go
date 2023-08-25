package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/godoji/candlestick"
	"io"
	"kio/internal/config"
	"log"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

var marketInfoCache *candlestick.ExchangeList = nil
var marketInfoCacheLock = sync.Mutex{}
var symbolsCache []string = nil
var symbolsCacheLock = sync.Mutex{}

var (
	ErrCandleServiceUnavailable = errors.New("candle provider encountered an error")
)

func SymbolList() ([]string, error) {
	symbolsCacheLock.Lock()
	if symbolsCache == nil {
		info, err := MarketInfo()
		if err != nil {
			return nil, err
		}
		symbols := make([]string, 0)
		for _, exchange := range info.Exchanges {
			for symbol := range exchange.Symbols {
				symbols = append(symbols, symbol)
			}
		}
		symbolsCache = symbols
	}
	symbolsCacheLock.Unlock()
	return symbolsCache, nil
}

func ClearMarketInfoCache() {
	marketInfoCacheLock.Lock()
	marketInfoCache = nil
	log.Println("market info cache evicted, waiting 30s before retrieving new data")
	time.Sleep(30 * time.Second)
	marketInfoCacheLock.Unlock()
}

func OnBoardDate(symbol string) (int64, error) {
	info := AssetInfo(symbol)
	if info == nil {
		return -1, errors.New("symbol info returned nil")
	}
	return info.OnBoardDate, nil
}

func ExchangeInfo(exchangeId string) *candlestick.ExchangeInfo {
	info, err := MarketInfo()
	if err != nil {
		return nil
	}
	for _, exchange := range info.Exchanges {
		if exchange.ExchangeId == exchangeId {
			return exchange
		}
	}
	return nil
}

func AssetInfo(symbol string) *candlestick.AssetInfo {
	identifier, ok := candlestick.ParseSymbol(symbol)
	if !ok {
		return nil
	}
	info, err := MarketInfo()
	if err != nil {
		return nil
	}
	for _, exchange := range info.Exchanges {
		if exchange.BrokerId != identifier.Broker {
			continue
		}
		if exchange.ExchangeId != identifier.Exchange {
			continue
		}
		return exchange.Symbols[symbol]
	}
	return nil
}

func MarketInfo() (*candlestick.ExchangeList, error) {

	marketInfoCacheLock.Lock()
	defer marketInfoCacheLock.Unlock()

	if marketInfoCache == nil {

		// fetch
		resp, err := http.Get(fmt.Sprintf("%s/market/info", config.ServiceConfig().DataBridgeURL()))
		if err != nil {
			return nil, err
		}

		// decode
		result := new(candlestick.ExchangeList)
		err = json.NewDecoder(resp.Body).Decode(result)

		// drain and close body
		if resp.Body != nil {
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				log.Print(err)
			}
			if err := resp.Body.Close(); err != nil {
				log.Print(err)
			}
		}

		if err != nil {
			return nil, err
		}

		log.Printf("retrieved exchange info containing %d exchanges\n", len(result.Exchanges))

		for _, exchange := range result.Exchanges {
			for _, info := range exchange.Symbols {
				if info.OnBoardDate != math.MinInt64 {
					continue
				}
				// TODO: fix this
				if v, ok := OldestBlock(info.Identifier.ToString()); ok {
					info.OnBoardDate = v
				}
			}
		}

		marketInfoCache = result
	}

	return marketInfoCache, nil
}

type requestCandlesResponse struct {
	Candles []candlestick.Candle `json:"candles"`
}

func fetchAlignedCandles(block int64, symbol string, interval int64) ([]candlestick.Candle, error) {

	results := make([]candlestick.Candle, 0)
	fetchStartTime := candlestick.BlockToUnix(block, interval)
	blockEndTime := candlestick.BlockToUnix(block+1, interval) - interval
	attempts := 0

	for true {
		// request candles
		candles, err := requestCandlesAligned(fetchStartTime, symbol, interval)
		if err != nil {
			return nil, err
		}

		// append them to result list
		if len(results) == 0 {
			results = candles
		} else {
			results = append(results, candles...)
		}

		// check if we got enough candles for at least one block, otherwise fetch from last candle
		lastCandleTime := results[len(results)-1].Time
		if lastCandleTime >= blockEndTime {
			break
		} else {
			fetchStartTime = lastCandleTime + interval
		}

		// make sure we don't send requests forever
		attempts++
		if attempts > 20 {
			log.Fatalf("too many attempts fetching %s till %d\n", symbol, blockEndTime)
		}
	}

	return results, nil
}

func requestCandlesAligned(from int64, symbol string, interval int64) ([]candlestick.Candle, error) {

	timerStart := time.Now().UTC().UnixMilli()
	resp, err := http.Get(fmt.Sprintf("%s/market/%s/historical?interval=%d&from=%d", config.ServiceConfig().DataBridgeURL(), symbol, interval, from))
	elapsed := time.Now().UTC().UnixMilli() - timerStart

	// check request error
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		message := ""
		if resp.Body != nil {
			raw, _ := io.ReadAll(resp.Body)
			message = ": " + string(raw)
			_ = resp.Body.Close()
		}
		return nil, errors.New(fmt.Sprintf("request for %s with interval %d returned status %d%s", symbol, interval, resp.StatusCode, message))
	}

	// decode
	result := new(requestCandlesResponse)
	err = json.NewDecoder(resp.Body).Decode(result)

	// drain and close body
	if resp.Body != nil {
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			log.Print(err)
		}
		if err := resp.Body.Close(); err != nil {
			log.Print(err)
		}
	}

	// check decode errors
	if err != nil {
		return nil, err
	}

	// make sure this request always takes a minimum of 3s
	// TODO: make rate limiting more reliable
	symbolParts := strings.Split(symbol, ":")
	if symbolParts[1] == "SPOT" {
		time.Sleep(time.Duration(500-elapsed) * time.Millisecond)
	} else if symbolParts[1] == "PERP" {
		time.Sleep(time.Duration(1500-elapsed) * time.Millisecond)
	} else {
		time.Sleep(time.Duration(1000-elapsed) * time.Millisecond)
	}

	return result.Candles, nil
}

func RequestCandlesFromTime(startTime int64, symbol string) ([]candlestick.Candle, error) {

	// send request
	resp, err := http.Get(fmt.Sprintf("%s/market/%s/latest?from=%d", config.ServiceConfig().DataBridgeURL(), symbol, startTime))
	if err != nil {
		return nil, err
	}

	// check status code
	if resp.StatusCode != http.StatusOK {
		return nil, ErrCandleServiceUnavailable
	}

	// decode json response
	result := new(requestCandlesResponse)
	err = json.NewDecoder(resp.Body).Decode(result)

	// drain and close body
	if resp.Body != nil {
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			log.Print(err)
		}
		if err := resp.Body.Close(); err != nil {
			log.Print(err)
		}
	}

	// check decode errors
	if err != nil {
		return nil, err
	}

	return result.Candles, nil

}
