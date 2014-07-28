package mesos

import (
	"net"
	"os"

	"github.com/golang/glog"
	"github.com/twitter/gozer/proto/mesos.pb"
	"github.com/twitter/gozer/proto/scheduler.pb"
)

func newDriver(mc *DriverConfig) (d *Driver, err error) {
	name, err := os.Hostname()
	if err != nil {
		return
	}

	addrs, err := net.LookupHost(name)
	if err != nil {
		return
	}
	glog.V(1).Infof("XXX: %s = %+v", name, addrs)

	d = &Driver{
		config:  *mc,
		command: make(chan func(*Driver) error),
		events:  make(chan *mesos_scheduler.Event, 100),
		Offers:  make(chan *mesos.Offer, 100),
		Updates: make(chan *TaskStateUpdate),
		pidIp:   addrs[0],
		pidPort: 8888, // TODO(weingart): use ephemeral port
	}

	return
}

func New(framework, user, master string, port int) (d *Driver, err error) {
	cf := &DriverConfig{
		FrameworkName:  framework,
		RegisteredUser: user,
		Masters: []MasterAddress{
			MasterAddress{Hostname: master, Port: port},
		},
	}

	if d, err = newDriver(cf); err == nil {
		go d.Run()
	}

	return
}
