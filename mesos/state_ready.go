package mesos

import (
	"time"

	"github.com/golang/glog"
)

const heartbeatTime = time.Minute

func stateReady(d *Driver) stateFn {
	glog.Info("driver.ready: framework is registered and waiting")

	select {
	case <-time.Tick(heartbeatTime):
		return stateHeartbeat

	case command := <-d.command:
		stateSendCommand := func(fm *Driver) stateFn {
			if err := command(fm); err != nil {
				glog.Error("driver.ready: failed to run command: ", err)
				return stateError
			}
			return stateReady
		}
		return stateSendCommand

	case event := <-d.events:
		stateReceiveEvent := func(fm *Driver) stateFn {
			if err := fm.eventDispatch(event); err != nil {
				glog.Error("driver.ready: failed to dispatch event: ", err)
				return stateError
			}
			return stateReady
		}
		return stateReceiveEvent
	}
}
