package database

import (
	"fmt"
	"github.com/dgraph-io/ristretto"
	"log"
)

var candleSetCache *ristretto.Cache
var candleSetCost = int64(360512)

func init() {
	var err error
	candleSetCache, err = ristretto.NewCache(&ristretto.Config{
		NumCounters: 1 << 12,                   // 8k items
		MaxCost:     (1 << 12) * candleSetCost, // 3GB
		BufferItems: 64,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func getCacheKey(symbol string, block int64, interval int64) string {
	return fmt.Sprintf("%s_%d_%d", symbol, block, interval)
}
