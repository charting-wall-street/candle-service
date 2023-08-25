package web

import (
	"encoding/gob"
	"encoding/json"
	"github.com/godoji/candlestick"
	"log"
	"net/http"
	"strings"
)

func sendAsJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(data)
}

func sendAsBinary(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_ = gob.NewEncoder(w).Encode(data)
}

func sendResponseCandles(w http.ResponseWriter, r *http.Request, data *candlestick.CandleSet) {

	// try to satisfy accept header
	accepts := r.Header.Get("Accept")

	// write in custom binary format if it is a binary stream request
	if strings.Index(accepts, "application/octet-stream") != -1 {
		payload, err := candlestick.EncodeCandleSet(data)
		if err != nil {
			log.Println(err)
			http.Error(w, "could not encode dataset: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(payload)
		return
	}

	sendResponse(w, r, data)

}

func sendResponse(w http.ResponseWriter, r *http.Request, data interface{}) {

	// try to satisfy accept header
	accepts := r.Header.Get("Accept")

	// send as json when nothing is specified
	if accepts == "" {
		sendAsJSON(w, data)
		return
	}

	// send as json when json is requested
	if strings.Index(accepts, "application/json") != -1 {
		sendAsJSON(w, data)
		return
	}

	// send as gob when binary is requested
	if strings.Index(accepts, "application/octet-stream") != -1 {
		sendAsBinary(w, data)
		return
	}

	// send as json when any is requested
	if strings.Index(accepts, "*/*") != -1 {
		sendAsJSON(w, data)
		return
	}

	// deny other types
	w.WriteHeader(http.StatusNotAcceptable)

}
