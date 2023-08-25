package database

import (
	"github.com/godoji/candlestick"
	"log"
	"math"
	"sync"
)

func RunRealTimeService() (chan interface{}, chan interface{}) {
	stop := make(chan interface{})
	done := make(chan interface{})
	go func() {
		<-stop
		// maintainRealTimeBlocks(stop) TODO: re-enable when fixed
		done <- nil
	}()
	return stop, done
}

var isTerminated = false
var lastUpdate = int64(0)

func checkTerminated(stop chan interface{}) bool {
	if isTerminated {
		return true
	}
	select {
	case _ = <-stop:
		isTerminated = true
		log.Println("stopping real-time service")
	default:
	}
	return isTerminated
}

var realTimeCache = make(map[string]*candlestick.CandleSet)
var realTimeCacheLock = sync.Mutex{}

func CacheBlockNumber(resolution int64) int64 {
	if lastUpdate == 0 {
		return math.MinInt64
	}
	return candlestick.UnixToBlock(lastUpdate, resolution)
}

func RealTimeBlock(symbol string, block int64) *candlestick.CandleSet {
	realTimeCacheLock.Lock()
	data := realTimeCache[symbol]
	realTimeCacheLock.Unlock()
	if data == nil || data.BlockNumber() != block {
		return nil
	}
	return data
}

//const cycleLength = int64(20) // number of seconds per fetch cycle
// var fetchHasFailed = 0
//
//func maintainRealTimeBlocks(stop chan interface{}) {
//
//	log.Println("starting real time service")
//
//	// keep track of first block that needs to be fetched
//	lastCycle := time.Now().UTC().Unix()
//
//	// multi thread
//	threads := threading.NewThreader(8)
//
//	// make sure we don't collide with the historical fetcher
//	for true {
//		if !historical.IsHistoryBusy() {
//			break
//		}
//		time.Sleep(250 * time.Millisecond)
//	}
//
//	// check if service hasn't stopped
//	if checkTerminated(stop) {
//		return
//	}
//
//	// fetch last available block and store in memory
//	info, err := store.MarketInfo()
//	if err != nil {
//		log.Println("could not retrieve market info")
//		log.Fatal(err)
//	}
//
//	// fetch all blocks
//	for sym, s := range info.Symbols {
//
//		if !historical.IsTrackedSymbol(sym) {
//			continue
//		}
//
//		block := lastCycle / 60 / 5000
//
//		// check if any metadata exists
//		meta, err := store.BlockMeta(s.Identifier.ToString(), block, 60)
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		// if metadata exist check if it is recent enough to continue real-time service
//		if meta != nil && lastCycle-meta.LastUpdate <= 60*60 {
//			data, err := store.LoadFromDisk(s.Identifier.ToString(), block, 60)
//			if err != nil {
//				log.Fatal(err)
//			}
//			if data != nil {
//				// log.Printf("recent data was found for %s, skipping download\n", s.Symbol)
//				realTimeCacheLock.Lock()
//				realTimeCache[s.Identifier.ToString()] = data
//				realTimeCacheLock.Unlock()
//				continue
//			}
//		}
//
//		// fetch full block if no data exists
//		threads.RunWithString(func(symbol string) {
//			blocks, err := store.DownloadBlocks(block, symbol)
//			if err != nil {
//				log.Fatal(err)
//			}
//			err = store.WriteToDisk(blocks[0])
//			if err != nil {
//				log.Fatal(err)
//			}
//			realTimeCacheLock.Lock()
//			realTimeCache[symbol] = blocks[0]
//			realTimeCacheLock.Unlock()
//		}, s.Identifier.ToString())
//
//		// check if stop was triggered
//		if checkTerminated(stop) {
//			break
//		}
//	}
//
//	// wait for final requests
//	threads.Wait()
//
//	// stop is cancelled
//	if isTerminated {
//		return
//	}
//
//	// fetch new candles every 20s
//	candleFetchLoop(stop)
//}
//
//func fetchAllLatestCandles() {
//
//	// fetch latest market info
//	info, err := store.MarketInfo()
//	if err != nil {
//		log.Println("failed to update market info, waiting 30s till next fetch")
//		time.Sleep(30 * time.Second)
//		return
//	}
//
//	// fetch around 90 min of candles and update the cached segment
//	threads := threading.NewThreader(8)
//	threads.Limit(25)
//	symbolMap := make(map[string]bool)
//	for _, s := range info.Symbols {
//		symbolMap[s.Identifier.ToString()] = true
//		threads.RunWithString(fetchSymbolCandles, s.Identifier.ToString())
//	}
//	threads.Wait()
//
//	// clear unused cache entries
//	realTimeCacheLock.Lock()
//	unused := make([]string, 0)
//	for symbol := range realTimeCache {
//		if _, ok := symbolMap[symbol]; !ok {
//			unused = append(unused, symbol)
//		}
//	}
//	for _, symbol := range unused {
//		delete(realTimeCache, symbol)
//	}
//	realTimeCacheLock.Unlock()
//
//}
//
//func candleFetchLoop(stop chan interface{}) {
//
//	// fetch cycles until stop signal
//	for {
//
//		// check termination
//		if checkTerminated(stop) {
//			break
//		}
//
//		// fetch
//		cycleStart := time.Now().UTC().Unix() - 1
//		fetchAllLatestCandles()
//		lastUpdate = cycleStart
//
//		// wait cycle length
//		for time.Now().UTC().Unix()-cycleStart < cycleLength {
//			if checkTerminated(stop) {
//				break
//			}
//			time.Sleep(500 * time.Millisecond)
//		}
//
//	}
//
//	// write cached blocks to disk before shutting down
//	for s := range realTimeCache {
//		if err := store.WriteToDisk(realTimeCache[s]); err != nil {
//			log.Println(err)
//		}
//	}
//
//	// send stop signal if this is an unexpected termination
//	if !isTerminated {
//		<-stop
//	}
//
//	log.Println("real-time service stopped")
//
//}
//
//func fetchSymbolCandles(symbol string) {
//
//	// fetch active candle set from cache
//	realTimeCacheLock.Lock()
//	segment, ok := realTimeCache[symbol]
//	realTimeCacheLock.Unlock()
//	if !ok {
//		log.Fatalf("cache entry not found for %s\n", symbol)
//	}
//
//	// check if process was asleep, we can fetch at most 99 candles, making 90 a good threshold
//	now := time.Now().UTC().Unix()
//	if now-segment.LastUpdate() > 60*90 {
//		log.Fatalf("cache block %d of %s is too outdated\n", segment.BlockNumber(), segment.Symbol())
//	}
//
//	// fetch candles
//	candles, err := store.RequestCandlesFromTime(segment.LastUpdate()-5*60, symbol)
//	if err != nil {
//		fetchHasFailed += 1
//		if fetchHasFailed <= 5 {
//			log.Printf("request failed for %s\n", symbol)
//		} else if fetchHasFailed == 6 {
//			log.Printf("more requests have failed but are not displayed\n")
//		}
//		return
//	} else {
//		if fetchHasFailed > 0 {
//			fetchHasFailed = 0
//			log.Println("new candles were updated successfully")
//		}
//	}
//
//	// update candles
//	if len(segment.Candles) == 0 {
//		log.Printf("memory candle set %d for %s is empty\n", segment.BlockNumber(), symbol)
//	}
//	if len(candles) == 0 {
//		log.Printf("latest candle set %d for %s is empty\n", segment.BlockNumber(), symbol)
//		store.ClearMarketInfoCache()
//	}
//	j := 0
//	i := 0
//	for {
//		if j == len(candles) || i == len(segment.Candles) {
//			break
//		}
//		if segment.Candles[i].Time < candles[j].Time {
//			i++
//		} else if segment.Candles[i].Time > candles[j].Time {
//			j++
//		} else {
//			segment.Candles[i] = candles[j]
//			j++
//			i++
//		}
//	}
//
//	// check if last candles will complete active block
//	segment.Meta.Complete = now >= ((segment.BlockNumber() + 1) * 5000 * 60)
//
//	// update timestamp
//	segment.Meta.LastUpdate = now
//
//	// remove cache entry
//	clearCacheEntry(segment.Symbol(), segment.BlockNumber())
//
//	// switch to next segment
//	if segment.Meta.Complete {
//
//		log.Printf("completed segment for %s\n", symbol)
//
//		// store completed segment to disk
//		err = store.WriteToDisk(segment)
//		if err != nil {
//			log.Printf("could not write completed cache block to disk for %s\n", symbol)
//			log.Fatal(err)
//		}
//
//		// create new segment
//		nextBlock := segment.BlockNumber() + 1
//		segment = &candlestick.CandleSet{
//			Candles: make([]candlestick.Candle, 5000),
//			Meta: candlestick.DataSetMeta{
//				UID:        symbol + ":60:" + strconv.FormatInt(nextBlock, 10),
//				Block:      nextBlock,
//				Complete:   false,
//				LastUpdate: now,
//				Symbol:     symbol,
//				Interval:   60,
//			},
//		}
//
//		// adjust time on candles
//		lastOpen := nextBlock*5000*60 - 60
//		for i := range segment.Candles {
//			lastOpen += 60 // add one minute
//			segment.Candles[i] = candlestick.Candle{
//				Time:    lastOpen,
//				Missing: true,
//			}
//		}
//
//		// try to fill in existing candles
//		j = 0
//		i = 0
//		for {
//			if j == len(candles) || i == len(segment.Candles) {
//				break
//			}
//			if segment.Candles[i].Time < candles[j].Time {
//				i++
//			} else if segment.Candles[i].Time > candles[j].Time {
//				j++
//			} else {
//				segment.Candles[i] = candles[j]
//				j++
//				i++
//			}
//		}
//
//		realTimeCacheLock.Lock()
//		realTimeCache[symbol] = segment
//		realTimeCacheLock.Unlock()
//	}
//}

func LastUpdateTime() int64 {
	return lastUpdate
}
