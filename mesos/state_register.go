package mesos

import (
	"github.com/golang/glog"
	"github.com/twitter/gozer/proto/mesos.pb"
	"github.com/twitter/gozer/proto/scheduler.pb"
)

func stateRegister(d *Driver) stateFn {
	glog.Info("driver.register: trying to register framework:", d)

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
		glog.Error("driver.register: failed to send register: ", err)
		return stateError
	}

	// Wait for Registered event, throw away any other events
	for {
		event := <-d.events
		if *event.Type != mesos_scheduler.Event_REGISTERED {
			glog.Warningf("driver.register: unexpected event type: want %q, got %+v",
				mesos_scheduler.Event_REGISTERED, *event.Type)
		}
		d.frameworkId = *event.Registered.FrameworkId
		break
	}

	glog.Infof("driver.register: registered %s:%s with id %q",
		d.config.RegisteredUser, d.config.FrameworkName, *d.frameworkId.Value)
	return stateReady
}
