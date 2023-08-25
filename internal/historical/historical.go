package historical

import (
	"github.com/godoji/candlestick"
	"kio/internal/config"
	"kio/internal/store"
	"log"
	"sync"
	"time"
)

var marketSymbolList = make(map[string]bool)
var historyBusy = true

func IsHistoryBusy() bool {
	return historyBusy
}

func RunHistoryAuditService() (chan interface{}, chan interface{}) {
	stop := make(chan interface{})
	done := make(chan interface{})
	go func() {
		if config.ServiceConfig().HasAudit() {
			fetchPastBlocks(stop)
		} else {
			<-stop
		}
		done <- nil
	}()
	return stop, done
}

func IsTrackedSymbol(symbol string) bool {
	return marketSymbolList[symbol]
}

func fetchBlocksForSymbol(symbol string, startTime int64, interval int64, isAborted *bool, stop chan interface{}) {

	timerStart := time.Now().Unix()
	endBlock := candlestick.UnixToBlock(time.Now().UTC().Unix(), interval)
	startBlock := candlestick.UnixToBlock(startTime, interval)

	// log.Printf("fetching %s range %d-%d (%d)\n", symbol.Symbol, startBlock, endBlock, endBlock-startBlock)
	for i := startBlock; i < endBlock; i++ {

		// listen for kill signal
		select {
		case _ = <-stop:
			log.Println("stopping history service")
			*isAborted = true
		default:
		}
		if *isAborted {
			return
		}

		// check if file has already been downloaded
		existing, err := store.BlockMeta(symbol, i, interval)
		if err != nil {
			log.Printf("failed meta reading %s block %d to disk\n", symbol, i)
			log.Fatal(err)
		}
		if existing != nil && existing.Complete {
			continue
		}

		// download the block which returns the next block in line
		i = store.DownloadBlocksToDisk(i, symbol, interval)
	}

	elapsed := time.Now().Unix() - timerStart
	if elapsed > 5 {
		log.Printf("history request for %s took %ds\n", symbol, elapsed)
	}
}

func fetchPastBlocks(stop chan interface{}) {

	log.Println("database audit has started")

	info, err := store.MarketInfo()
	if err != nil {
		log.Println("could not retrieve market info")
		log.Fatal(err)
	}

	for _, e := range info.Exchanges {
		for s := range e.Symbols {
			marketSymbolList[s] = true
		}
	}

	cancel := false

	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, exchange := range info.Exchanges {
		for symbol := range exchange.Symbols {
			for _, interval := range exchange.Resolution {
				wg.Add(1)
				sem <- struct{}{}
				go func(symbol string, info *candlestick.ExchangeInfo, interval int64) {
					defer wg.Done()
					meta, ok := info.Symbol(symbol)
					if !ok {
						log.Fatalf("symbol info not found for %s\n", symbol)
					}
					startTime := meta.OnBoardDate
					fetchBlocksForSymbol(symbol, startTime, interval, &cancel, stop)
					<-sem
				}(symbol, exchange, interval)
			}
		}
	}
	wg.Wait()

	historyBusy = false
	log.Println("database audit completed")

	if !cancel {
		<-stop
	}
	log.Println("history service has terminated")
}
