package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"

	"github.com/twitter/gozer/mesos"
	mesos_pb "github.com/twitter/gozer/proto/mesos.pb"
)

const (
	frameworkName = "gozer"
)

var (
	user = flag.String("user", "", "The user to register as")
	port = flag.Int("api_port", 12345, "Port to listen on for API")

	httpWaitGroup sync.WaitGroup

	tasks = make(chan Task)

	taskIndex = 0

	taskStates = make(map[string]mesos_pb.TaskState)

	eventChannels = mesos.NewEventChannels()
)

type Task struct {
	Command string
	// TODO(dhamon): resource requirements
}

func listen() {
	mesos.Listen()

	// TODO(dhamon): root handler to show status of tasks.
	http.HandleFunc("/add", addTask)
	httpWaitGroup.Add(1)
	go func() {
		log.Printf("%s listening on port %d", frameworkName, *port)
		httpWaitGroup.Done()
		if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
			log.Fatalf("failed to start listening on port %d", *port)
		}
	}()
}

func addTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Add("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("received add request with unexpected method. want %q, got %q: %+v", "POST", r.Method, r)
		return
	}

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("failed to read body from add request %+v: %+v", r, err)
		// TODO(dhamon): Better error for this case.
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	var task Task
	err = json.Unmarshal(body, &task)

	if err != nil {
		log.Printf("failed to parse JSON body from add request %+v: %+v", r, err)
		// TODO(dhamon): Better error for this case.
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Printf("received task: %+v", task)
	tasks <- task

	w.WriteHeader(http.StatusOK)
}

func handleStatusUpdates() {
	for {
		update := <-eventChannels.Updates

		if _, ok := taskStates[update.TaskId]; ok {
			taskStates[update.TaskId] = update.State

			switch update.State {
			case mesos_pb.TaskState_TASK_RUNNING:
				log.Printf("task %s is running", update.TaskId)
				break
			case mesos_pb.TaskState_TASK_FINISHED:
				log.Printf("task %s is complete", update.TaskId)
				break
			case mesos_pb.TaskState_TASK_FAILED:
			case mesos_pb.TaskState_TASK_KILLED:
			case mesos_pb.TaskState_TASK_LOST:
				log.Printf("task %s failed to complete", update.TaskId)
			}
			err := mesos.SendAck(update)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Printf("received update for unknown task %q", update.TaskId)
		}
	}
}

func handleOffers() {
	for {
		offer := <-eventChannels.Offers

		log.Printf("received offer %+v", offer)
		log.Printf("waiting for tasks...")
		task := <-tasks

		// TODO(dhamon): Decline offers if resources don't match.
		taskId := fmt.Sprintf("gozer-task-%d", taskIndex)
		err := mesos.LaunchTask(offer, taskId, task.Command)
		if err != nil {
			log.Fatal(err)
		}
		taskIndex += 1
		taskStates[taskId] = mesos_pb.TaskState_TASK_STAGING

		// TODO(dhamon): return unused resources
	}
}

func main() {
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// Start API
	listen()
	httpWaitGroup.Wait()

	// Set up handlers
	go handleStatusUpdates()
	go handleOffers()

	log.Printf("registering...")
	err := mesos.Register(*user, frameworkName)
	if err != nil {
		log.Fatal(err)
	}

	// Start event loop.
	err = eventChannels.Run()
	if err != nil {
		log.Fatal(err)
	}
}
