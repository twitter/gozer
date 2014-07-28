package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/twitter/gozer/mesos"
)

var (
	user       = flag.String("user", "", "The user to register as")
	port       = flag.Int("port", 4343, "Port to listen on for the API endpoint")
	master     = flag.String("master", "localhost", "Hostname of the master")
	masterPort = flag.Int("masterPort", 5050, "Port of the master")

	taskstore = NewTaskStore()

	// TODO(dhamon): add custom logger
)

type TaskState int

const (
	TaskState_UNKNOWN TaskState = iota
	TaskState_INIT
	TaskState_STARTING
	TaskState_RUNNING
	TaskState_FAILED
	TaskState_FINISHED
)

func (t TaskState) String() string {
	switch t {
	case TaskState_INIT:
		return "INIT"
	case TaskState_STARTING:
		return "STARTING"
	case TaskState_RUNNING:
		return "RUNNING"
	case TaskState_FAILED:
		return "FAILED"
	case TaskState_FINISHED:
		return "FINISHED"
	default:
		return "UNKNOWN"
	}
}

type Task struct {
	Id        string           `json:"id"`
	Command   string           `json:"command"`
	State     TaskState        `json:"state"`
	mesosTask *mesos.MesosTask `json:"-"`
	// TODO(dhamon): resource requirements
}

func (t Task) String() string {
	return fmt.Sprintf("Task {id: %q, command: %q, state: %q}", t.Id, t.Command, t.State)
}

func startAPI() {
	http.HandleFunc("/api/addtask", addTaskHandler)
	glog.Infof("gozer", "api listening on port %d", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
		glog.Fatalf("gozer", "failed to start listening on port %d", *port)
	}
}

func addTaskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.Header().Add("Allow", "POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
		glog.Errorf("gozer", "received addtask request with unexpected method. want %q, got %q: %+v", "POST", r.Method, r)
		return
	}
	defer r.Body.Close()

	var task Task
	err := json.NewDecoder(r.Body).Decode(&task)
	if err != nil {
		glog.Errorf("gozer", "failed to parse JSON body from addtask request %+v: %+v", r, err)
		// TODO(dhamon): Better error for this case.
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if len(task.Command) == 0 {
		glog.Errorf("gozer", "cannot launch task with empty command: %s", task)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	taskstore.Add(&task)

	w.WriteHeader(http.StatusOK)
}

func main() {
	flag.Parse()

	go startAPI()

	driver, err := mesos.New("gozer", *user, *master, *masterPort)
	if err != nil {
		glog.Fatalf("Failed to create mesos driver: %+v", err)
	}

	// Shepherd all our tasks
	//
	// Note: This will require a significant re-architecting, most likely to break out the
	// gozer based tasks and their state transitions, possibly using a go-routine per task,
	// which may limit the total number of tasks we can handle (100k go-routines might be
	// too much).  It should make for a simple abstraction, where the update routine should
	// then be able to simply use a channel to post a state transition to the gozer task,
	// which the gozer task manager (per task) go routine would then use to transition the
	// task through its state diagram.  This should also make it very simple to detect bad
	// transitions.
	//
	// It would be nice if we could just only use the mesos.TaskState_TASK_* states, however,
	// they do not encompass the ideas of PENDING (waiting for offers), and ASSIGNED (offer
	// selected, waiting for running), nor do they encompass tear-down and death.
	//
	// For now we use a simple loop to do a very naive management of tasks, updates, events,
	// errors, etc.
	for {

		select {

		case update := <-driver.Updates:
			glog.Infof("gozer", "status update received: %s", update)
			update.Ack()

		// TODO(dhamon): Flip this around so we check for offers and then compare to
		// pending tasks.
		case <-time.After(5 * time.Second):
			glog.V(1).Info("gozer", "checking for tasks")
			// After a timeout, see if there any tasks to launch
			taskIds := taskstore.Ids()
			for _, taskId := range taskIds {
				state, err := taskstore.State(taskId)
				if err != nil {
					glog.Errorf("gozer", "failed to get state for task %q: %+v", taskId, err)
					continue
				}

				if state != TaskState_INIT {
					continue
				}

				mesosTask, err := taskstore.MesosTask(taskId)
				if err != nil {
					glog.Errorf("gozer", "failed to get mesos task for task %q: %+v", taskId, err)
					continue
				}

				// Start this task (very naive method)
				offer := <-driver.Offers

				// TODO(dhamon): Check for resources before launching
				err = driver.LaunchTask(offer, mesosTask)
				if err != nil {
					glog.Errorf("gozer", "failed to launch task %q: %+v", taskId, err)
					continue
				}

				taskstore.Update(taskId, TaskState_STARTING)
			}
		}
	}
}
