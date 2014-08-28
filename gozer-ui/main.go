package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"

	"github.com/twitter/gozer/gozer"
)

const (
	rootHTML = `
<html>
  <head>
    <title>gozer</title>
  </head>
  <body>
    <h1>gozer</h1>
    <h2>tasks</h2>
    <table>
	<tr>
		<th>id</th><th>command</th><th>state</th>
	</tr>
	{{range $task := .}}
	<tr>
		<td>{{$task.Id}}</td><td>{{$task.Command}}</td><td>{{$task.State}}</td>
	</tr>
	{{end}}
  </body>
</html>
`
)

var (
	port		= flag.Int("port", 5000, "Port to listen on")
	gozerHostname	= flag.String("gozerHostname", "localhost", "Hostname of gozer")
	gozerPort	= flag.Int("gozerPort", 4343, "Port Gozer's API is listening on")

	rootTemplate	= template.Must(template.New("root").Parse(rootHTML))
)

func main() {
	flag.Parse()

	http.HandleFunc("/", rootHandler)
	log.Printf("Listening on port %d", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
		log.Fatalf("Failed to start listening on port %d", *port)
	}
}

func makeGozerUrl(path string) string {
	return fmt.Sprintf("http://%s:%d/%s", *gozerHostname, *gozerPort, path)
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	tasksUrl := makeGozerUrl("tasks")
	resp, err := http.Get(tasksUrl)
	if err != nil {
		log.Printf("Failed to get task information from gozer at %q", tasksUrl)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var taskStore []gozer.Task

	dec := json.NewDecoder(resp.Body)

	err = dec.Decode(&taskStore)
	if err != nil {
		if err == io.EOF {
			log.Printf("No task store found in response %+v", resp.Body)
			w.WriteHeader(http.StatusNotFound)
		} else {
			log.Printf("Failed to decode %+v into task store: %+v", resp.Body, err)
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	rootTemplate.Execute(w, taskStore)
}

