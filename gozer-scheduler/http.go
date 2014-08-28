package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/twitter/gozer/gozer"
)

var taskIndex = 0

func startHTTP() {
	http.HandleFunc("/tasks", tasksHandler)
	http.HandleFunc("/api/addtask", addTaskHandler)
	log.Info.Printf("API listening on port %d", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
		log.Error.Fatalf("Failed to start listening on port %d", *port)
	}
}

func addTaskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Add("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Error.Printf("Received addtask request with unexpected method. want %q, got %q: %+v", "POST", r.Method, r)
	}
	defer r.Body.Close()

	var task gozer.Task
	err := json.NewDecoder(r.Body).Decode(&task)
	if err != nil {
		log.Error.Printf("Failed to parse JSON body from addtask request %+v: %+v", r, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(task.Id) == 0 {
		task.Id = fmt.Sprintf("gozer-task-%d", taskIndex)
		taskIndex += 1
	}

	taskstore.Add(&Task{gozerTask: &task})

	w.WriteHeader(http.StatusOK)
}

func tasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(taskstore.tasks)
	if err != nil {
		log.Error.Printf("Failed to marshal %+v to JSON: %+v", taskstore.tasks, err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
