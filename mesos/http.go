package mesos

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

var (
	mesosPort	= flag.Int("mesos_port", 8888, "Port to listen on for mesos messages")
)

func (d Driver) Listen() {
	httpWaitGroup.Add(1)
	go func() {
		log.Printf("framework listening on port %d", *mesosPort)
		httpWaitGroup.Done()
		if err := http.ListenAndServe(fmt.Sprintf(":%d", *mesosPort), d); err != nil {
			log.Fatalf("failed to start framework listening on port %d", *mesosPort)
		}
	}()
}

func (d Driver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Add("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("received request with unexpected method. want %q, got %q: %+v", "POST", r.Method, r)
		return
	}

	pathElements := strings.Split(r.URL.Path, "/")

	if pathElements[1] != d.frameworkName {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf("unexpected path. want %q, got %q", d.frameworkName, pathElements[1])))
		log.Printf("received request with unexpected path. want %q, got %q: %+v", d.frameworkName, pathElements[1], r)
		return
	}

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("ERROR: failed to read body from request %+v: %+v", r, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	event, err := bytesToEvent(pathElements[2], body)
	if err != nil {
		log.Printf("ERROR: %+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	events <- event

	w.WriteHeader(http.StatusOK)
}
