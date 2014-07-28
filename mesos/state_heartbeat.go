package mesos

import (
	"github.com/golang/glog"
)

// We are reached here only from the 'Ready' state
func stateHeartbeat(d *Driver) stateFn {
	glog.V(1).Info("driver.state: heartbeat")
	return stateReady
}
