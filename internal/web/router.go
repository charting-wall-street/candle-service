package web

import (
	"github.com/godoji/candlestick"
	"github.com/gorilla/mux"
	"kio/internal/database"
	"kio/internal/store"
	"net/http"
	"strconv"
)

func getMarketInfo(w http.ResponseWriter, r *http.Request) {
	info, err := store.MarketInfo()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sendResponse(w, r, info)
}

type intervalsResponse struct {
	Intervals []int64 `json:"intervals"`
}

type symbolsResponse struct {
	Symbols []string `json:"symbols"`
}

func getIntervalList(w http.ResponseWriter, r *http.Request) {
	result := &intervalsResponse{
		Intervals: candlestick.IntervalList,
	}
	sendResponse(w, r, result)
}

func getSymbols(w http.ResponseWriter, r *http.Request) {
	symbols, err := store.SymbolList()
	if err != nil {
		http.Error(w, "could not find exchange info", http.StatusInternalServerError)
		return
	}
	result := &symbolsResponse{
		Symbols: symbols,
	}
	sendResponse(w, r, result)
}

func getCandles(w http.ResponseWriter, r *http.Request) {

	symbol := mux.Vars(r)["symbol"]
	if s := store.AssetInfo(symbol); s == nil {
		http.Error(w, "symbol not found", http.StatusNotFound)
		return
	}

	segmentS := r.URL.Query().Get("segment")
	segment, err := strconv.ParseInt(segmentS, 10, 64)
	if err != nil {
		http.Error(w, "invalid segment parameter", http.StatusBadRequest)
		return
	}

	intervalS := r.URL.Query().Get("interval")
	interval, err := strconv.ParseInt(intervalS, 10, 64)
	if err != nil {
		http.Error(w, "invalid interval parameter", http.StatusBadRequest)
		return
	}

	useCache := r.URL.Query().Get("cache") != "no-cache"

	results, err := database.FetchCandles(symbol, segment, interval, useCache)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if results == nil {
		http.Error(w, "data does not exist or hasn't been downloaded yet", http.StatusNotFound)
		return
	}

	sendResponseCandles(w, r, results)

}

func getTransition(w http.ResponseWriter, r *http.Request) {

	symbol := mux.Vars(r)["symbol"]
	if s := store.AssetInfo(symbol); s == nil {
		http.Error(w, "symbol not found", http.StatusNotFound)
		return
	}

	segmentS := r.URL.Query().Get("segment")
	segment, err := strconv.ParseInt(segmentS, 10, 64)
	if err != nil {
		http.Error(w, "invalid segment parameter", http.StatusBadRequest)
		return
	}

	intervalS := r.URL.Query().Get("interval")
	interval, err := strconv.ParseInt(intervalS, 10, 64)
	if err != nil {
		http.Error(w, "invalid interval parameter", http.StatusBadRequest)
		return
	}

	resolutionS := r.URL.Query().Get("resolution")
	resolution, err := strconv.ParseInt(resolutionS, 10, 64)
	if err != nil {
		http.Error(w, "invalid resolution parameter", http.StatusBadRequest)
		return
	}

	results, err := database.FetchTransition(symbol, segment, interval, resolution)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if results == nil {
		http.Error(w, "data does not exist or hasn't been downloaded yet", http.StatusNotFound)
		return
	}

	sendResponseCandles(w, r, results)
}

func lastCandleUpdate(w http.ResponseWriter, _ *http.Request) {
	s := strconv.FormatInt(database.LastUpdateTime(), 10)
	_, _ = w.Write([]byte(s))
}

func router() *mux.Router {

	r := mux.NewRouter()

	r.HandleFunc("/market/last-update", lastCandleUpdate).Methods("GET")
	r.HandleFunc("/market/info", getMarketInfo).Methods("GET")
	r.HandleFunc("/market/intervals", getIntervalList).Methods("GET")
	r.HandleFunc("/market/symbols", getSymbols).Methods("GET")
	r.HandleFunc("/market/t/{symbol}", getTransition).Methods("GET")
	r.HandleFunc("/market/{symbol}", getCandles).Methods("GET")

	return r
}
