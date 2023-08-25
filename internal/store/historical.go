package store

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/godoji/candlestick"
	"io"
	"kio/internal/config"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const blockDiskOffset = 1000000

func blockDiskPath(symbol string, block int64, resolution int64) (string, string, string) {
	diskBlock := block + blockDiskOffset
	dir := fmt.Sprintf("%s/db/%s/%d/%d", config.ServiceConfig().DataDir(), symbol, resolution, diskBlock/100)
	file := fmt.Sprintf("%s/%d.bin", dir, diskBlock)
	meta := fmt.Sprintf("%s/%d.meta.json", dir, diskBlock)
	return dir, file, meta
}

func DownloadBlocksToDisk(blockNumber int64, symbol string, interval int64) int64 {
	data, lastBlock, err := DownloadBlocks(blockNumber, symbol, interval)
	if err != nil {
		log.Fatal(err)
	}
	for _, b := range data {
		err = WriteToDisk(b)
		if err != nil {
			log.Printf("failed writing %s block %d to disk\n", b.Symbol(), b.BlockNumber())
			log.Fatal(err)
		}
	}
	return lastBlock
}

func DownloadBlocks(blockNumber int64, symbol string, interval int64) (map[int64]*candlestick.CandleSet, int64, error) {

	timerStart := time.Now().UnixMilli()

	// fetch a minimum of 5000 candles
	currentTime := time.Now().UTC().Unix()
	candles, err := fetchAlignedCandles(blockNumber, symbol, interval)
	largestBlock := blockNumber
	if err != nil {
		return nil, 0, err
	}

	// create candle sets based on how many candles we retrieved in a single request
	candleSets := make(map[int64]*candlestick.CandleSet, 0)
	for _, candle := range candles {
		candleBlock := candlestick.UnixToBlock(candle.Time, interval)
		if candleBlock < blockNumber {
			fmt.Printf("invalid block found for %s: expected lowest %d but got %d\n", symbol, blockNumber, candleBlock)
			continue
		}
		if candleBlock > largestBlock {
			largestBlock = candleBlock
		}
		set, ok := candleSets[candleBlock]
		if !ok {
			set = &candlestick.CandleSet{
				Candles: make([]candlestick.Candle, 0),
				Meta: candlestick.DataSetMeta{
					UID:        symbol + ":" + strconv.FormatInt(interval, 10) + ":" + strconv.FormatInt(candleBlock, 10),
					Block:      candleBlock,
					Complete:   currentTime >= candle.Time,
					LastUpdate: candle.Time,
					Symbol:     symbol,
					Interval:   interval,
				},
			}
			addedCandles := int64(0)
			t := candlestick.BlockToUnix(candleBlock, interval)
			for t != candle.Time {
				set.Candles = append(set.Candles, candlestick.Candle{
					Time:    t,
					Missing: true,
				})
				t += interval
				addedCandles++
				if addedCandles > candlestick.CandleSetSize {
					log.Fatalf("bad candle sequence detected: %s\n", symbol)
				}
			}
			candleSets[candleBlock] = set
		} else {
			if currentTime >= candle.Time {
				set.Meta.Complete = true
				set.Meta.LastUpdate = candle.Time
			}
		}
		set.Candles = append(set.Candles, candle)
	}

	for _, set := range candleSets {
		if len(set.Candles) == int(candlestick.CandleSetSize) {
			continue
		}
		addedCandles := int64(0)
		t := set.Candles[len(set.Candles)-1].Time + interval
		for len(set.Candles) != int(candlestick.CandleSetSize) {
			set.Candles = append(set.Candles, candlestick.Candle{
				Time:    t,
				Missing: true,
			})
			t += interval
			addedCandles++
			if addedCandles > candlestick.CandleSetSize {
				log.Fatalf("bad candle sequence detected: %s\n", symbol)
			}
		}
	}

	elapsed := time.Now().UnixMilli() - timerStart
	log.Printf("fetched %s block %d->%d in %dms\n", symbol, blockNumber, largestBlock, elapsed)

	return candleSets, largestBlock, nil
}

func WriteToDisk(data *candlestick.CandleSet) error {
	dir, dst, fMeta := blockDiskPath(data.Symbol(), data.BlockNumber(), data.Interval())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}
	file, err := os.Create(dst)
	if err != nil {
		return err
	}
	err = gob.NewEncoder(file).Encode(data)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		log.Println("failed closing file")
		log.Println(err)
	}

	file, err = os.Create(fMeta)
	if err != nil {
		return err
	}
	err = json.NewEncoder(file).Encode(data.Meta)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		log.Println("failed closing file")
		log.Println(err)
	}

	// log.Printf("wrote %s block %d (%d) to disk\n", data.Symbol(), data.BlockNumber(), data.Interval())
	return nil
}

func BlockMeta(symbol string, block int64, interval int64) (*candlestick.DataSetMeta, error) {

	_, _, fMeta := blockDiskPath(symbol, block, interval)

	if _, err := os.Stat(fMeta); os.IsNotExist(err) {
		return nil, nil
	}

	file, err := os.Open(fMeta)
	if err != nil {
		return nil, err
	}
	data := new(candlestick.DataSetMeta)
	err = json.NewDecoder(file).Decode(data)
	if err != nil {
		return nil, err
	}
	if _, err = io.Copy(io.Discard, file); err != nil {
		log.Print(err)
	}
	err = file.Close()
	if err != nil {
		log.Println(err)
	}

	return data, nil
}

func OldestBlock(symbol string) (int64, bool) {
	dir := fmt.Sprintf("%s/db/%s", config.ServiceConfig().DataDir(), symbol)
	blockLowerBound := int64(math.MaxInt64)
	err := filepath.Walk(dir,
		func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if strings.Index(filePath, ".bin") != -1 {
				parts := strings.Split(filePath, "/")
				fileName := parts[len(parts)-1]
				fileNameParts := strings.Split(fileName, ".")
				blockNumber, err := strconv.ParseInt(fileNameParts[0], 10, 64)
				if err != nil {
					return nil
				}
				interval, err := strconv.ParseInt(parts[len(parts)-3], 10, 64)
				if err != nil {
					return nil
				}
				blockNumber -= blockDiskOffset
				t := candlestick.BlockToUnix(blockNumber, interval)
				if t < blockLowerBound {
					blockLowerBound = t
				}
			}
			return nil
		})
	if err != nil {
		return 0, false
	}
	return blockLowerBound, true
}

func LoadFromDisk(symbol string, block int64, resolution int64) (*candlestick.CandleSet, error) {

	_, fName, _ := blockDiskPath(symbol, block, resolution)

	// fetch new data if local data does not exist
	if _, err := os.Stat(fName); os.IsNotExist(err) {
		return nil, nil
	}

	// open existing data file
	file, err := os.Open(fName)
	if err != nil {
		return nil, err
	}
	data := new(candlestick.CandleSet)
	err = gob.NewDecoder(file).Decode(data)
	if err != nil {
		return nil, err
	}
	if _, err = io.Copy(io.Discard, file); err != nil {
		log.Print(err)
	}
	err = file.Close()
	if err != nil {
		log.Println(err)
	}

	//log.Printf("read %s block %d (%d) to disk\n", data.Symbol(), data.Interval(), data.Interval())

	return data, nil
}
