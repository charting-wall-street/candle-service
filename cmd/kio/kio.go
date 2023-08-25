package main

import (
	"kio/internal/config"
	"kio/internal/database"
	"kio/internal/historical"
	"kio/internal/web"
	"log"
	"os"
	"os/signal"
)

func main() {

	log.Println("|- Candle IO Service -|")

	// parse command line args
	config.LoadConfig()

	// run threads
	serverDone, serverStop := web.RunHttpServer()
	historyStop, historyDone := historical.RunHistoryAuditService()
	rtStop, rtDone := database.RunRealTimeService()

	// create signals to listen to kill signal
	stop := make(chan os.Signal, 1)
	done := make(chan interface{})
	signal.Notify(stop, os.Interrupt, os.Kill)

	go func() {

		// listen for kill signal
		sig := <-stop
		log.Printf("received %s signal\n", sig)

		// stop threads
		serverStop <- nil
		historyStop <- nil
		rtStop <- nil

		// wait for threads to confirm stop
		<-serverDone
		<-historyDone
		<-rtDone

		// end program
		done <- nil

	}()

	<-done

	os.Exit(0)
}
