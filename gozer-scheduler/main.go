package main

import (
	"flag"
	"os"

	"github.com/twitter/gozer/mesos"
	"github.com/twitter/gozer/gozer"
)

var (
	user       = flag.String("user", "", "The user to register as")
	port       = flag.Int("port", 4343, "Port to listen on for the API endpoint")
	master     = flag.String("master", "localhost", "Hostname of the master")
	masterPort = flag.Int("masterPort", 5050, "Port of the master")

	taskstore = NewTaskStore()

	// TODO(dhamon): flags for log level
	log = mesos.NewLog(mesos.LogConfig{
		Prefix: "gozer",
		Info:   os.Stdout,
		Warn:   os.Stderr,
		Error:  os.Stderr},
	)
)

func main() {
	flag.Parse()

	go startHTTP()

	log.Info.Println("Registering")
	driver, err := mesos.New("gozer", *user, *master, *masterPort)
	if err != nil {
		log.Error.Fatal(err)
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
	exit := false
	for !exit {
		select {
		case update, ok := <-driver.Updates:
			if !ok {
				log.Info.Printf("Update channel closed. Exiting")
				exit = true
				break
			}
			log.Info.Printf("Received update: %+v", update)
			state, err := taskstore.State(update.TaskId)
			if err != nil {
				log.Error.Printf("Failed to get current state for updated task %q: %+s", update.TaskId, err)
				continue
			}

			newState, ok := gozer.TaskStateMap[update.State]
			if !ok {
				log.Error.Printf("Unknown mesos task state: %q", update.State)
				continue
			}

			log.Info.Printf("Updating task state from %q to %q", state, newState)
			if err := taskstore.Update(update.TaskId, newState); err != nil {
				log.Error.Print(err)
			}

			update.Ack()

		case offer, ok := <-driver.Offers:
			if !ok {
				log.Info.Printf("Offer channel closed. Exiting")
				exit = true
				break
			}
			log.Info.Printf("Received offer: %+v", offer)

			taskIds := taskstore.Ids()
			launched := false
			for _, taskId := range taskIds {
				state, err := taskstore.State(taskId)
				if err != nil {
					log.Error.Printf("Error getting task state for task %q: %+v", taskId, err)
					continue
				}

				if state != gozer.TaskState_INIT {
					continue
				}

				mesosTask, err := taskstore.MesosTask(taskId)
				if err != nil {
					log.Error.Printf("Error getting mesos task for task %q: %+v", taskId, err)
					continue
				}

				// TODO(dhamon): Check for resource match before launching.
				log.Info.Printf("Launching task %s", taskId)
				err = driver.LaunchTask(offer, mesosTask)
				if err != nil {
					log.Error.Printf("Error launching task %q: %+v", taskId, err)
					continue
				}

				taskstore.Update(taskId, gozer.TaskState_STARTING)
				launched = true
				break
			}

			if !launched {
				log.Info.Printf("Declining offer %s", offer.Id)
				offer.Decline()
			}
		}
	}
}
