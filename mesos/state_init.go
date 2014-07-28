package mesos

import (
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
)

const maxDelay = 2 * time.Minute

// We wait until HTTP Pid endpoint is ready and healthy
func stateInit(d *Driver) stateFn {
	glog.Info("driver.init: starting framework:", d)

	delay := time.Second
	healthURL := fmt.Sprintf("http://%s:%d/health", d.pidIp, d.pidPort)

	// Start Pid endpoint
	go startServing(d)

	// Now wait for healthy endpoint
	for {
		resp, err := http.Get(healthURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			// Ignore content
			break
		}

		glog.Info("driver.init: timeout for URL %q: %+v", healthURL, err)
		time.Sleep(delay)
		if delay < maxDelay {
			delay = delay * 2
		}
	}

	return stateRegister
}
