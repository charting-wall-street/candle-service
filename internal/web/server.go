package web

import (
	"context"
	"github.com/gorilla/handlers"
	"github.com/urfave/negroni"
	"kio/internal/config"
	"log"
	"net/http"
	"strings"
	"time"
)

func RunHttpServer() (chan interface{}, chan interface{}) {

	// Middleware and routes
	app := negroni.New(negroni.NewRecovery())
	app.UseHandler(router())

	// CORS
	headersOk := handlers.AllowedHeaders([]string{"X-Requested-With", "Content-Type", "Authorization", "Cache-Control"})
	exposedOk := handlers.ExposedHeaders([]string{"Link"})
	originsOk := handlers.AllowedOrigins(strings.Split(config.ServiceConfig().AllowedOrigins(), ","))
	methodsOk := handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS", "DELETE"})
	handler := handlers.CORS(originsOk, headersOk, methodsOk, exposedOk)(app)

	server := &http.Server{
		Addr:    ":" + config.ServiceConfig().Port(),
		Handler: handler,
	}

	done := make(chan interface{})
	stop := make(chan interface{})

	go func() {
		log.Printf("starting server on port %s\n", config.ServiceConfig().Port())
		if err := server.ListenAndServe(); err != nil {
			if http.ErrServerClosed != err {
				log.Println(err)
				done <- nil
			}
		}
	}()

	go func() {
		<-stop
		log.Println("stopping http server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := server.Shutdown(ctx)
		cancel()
		if err != nil {
			log.Print(err)
		}
		log.Println("server has terminated")
		done <- nil
	}()

	return done, stop

}
