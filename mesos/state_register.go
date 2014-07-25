package mesos

import (
	"log"

	"github.com/twitter/gozer/proto/mesos.pb"
	"github.com/twitter/gozer/proto/scheduler.pb"
)

func stateRegister(d *Driver) stateFn {
	log.Print("REGISTERING: Trying to register framework:", d)

	// Create the register message and send it.
	callType := mesos_scheduler.Call_REGISTER
	registerCall := &mesos_scheduler.Call{
		Type: &callType,
		FrameworkInfo: &mesos.FrameworkInfo{
			User: &d.config.RegisteredUser,
			Name: &d.config.FrameworkName,
		},
	}

	// TODO(weingart): This should re-try and backoff
	err := d.send(registerCall)
	if err != nil {
		log.Print("Error: send: ", err)
		return stateError
	}

	// Wait for Registered event, throw away any other events
	for {
		event := <-d.events
		if *event.Type != mesos_scheduler.Event_REGISTERED {
			log.Printf("Unexpected event type: want %q, got %+v", mesos_scheduler.Event_REGISTERED, *event.Type)
		}
		d.frameworkId = *event.Registered.FrameworkId
		break
	}

	log.Printf("Registered %s:%s with id %q", d.config.RegisteredUser, d.config.FrameworkName, *d.frameworkId.Value)
	return stateReady
}