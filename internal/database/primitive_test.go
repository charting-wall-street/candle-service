package database

import (
	"github.com/godoji/candlestick"
	"testing"
)

func TestCandleMerge(t *testing.T) {

	c1 := candlestick.Candle{
		Open:           100,
		High:           200,
		Low:            50,
		Close:          150,
		Volume:         100,
		TakerVolume:    100,
		NumberOfTrades: 10,
		Time:           1,
		Missing:        false,
	}

	c2 := candlestick.Candle{
		Open:           150,
		High:           300,
		Low:            100,
		Close:          125,
		Volume:         75,
		TakerVolume:    50,
		NumberOfTrades: 5,
		Time:           2,
		Missing:        false,
	}

	mergeCandles(&c2, &c1)

	if c1.Low != 50 {
		t.Fail()
	}
	if c1.High != 300 {
		t.Fail()
	}
	if c1.Open != 100 {
		t.Fail()
	}
	if c1.Close != 125 {
		t.Fail()
	}
	if c1.Volume != 175 {
		t.Fail()
	}
	if c1.TakerVolume != 150 {
		t.Fail()
	}
	if c1.NumberOfTrades != 15 {
		t.Fail()
	}
	if c1.Time != 1 {
		t.Fail()
	}

}
