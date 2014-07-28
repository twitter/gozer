package mesos

import (
	"github.com/golang/glog"
)

type stateErrorId int

const (
	errorNone stateErrorId = iota
	errorNotInitialized
	errorNotConnected
	errorNotReady
	//...
)

func stateError(d *Driver) stateFn {
	// Handle any type of error state
	glog.Error("driver.error:", d)
	return stateStop
}
