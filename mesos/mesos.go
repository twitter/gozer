package mesos

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/twitter/gozer/proto/mesos.pb"
	"github.com/twitter/gozer/proto/scheduler.pb"
)

var (
	master     = flag.String("master", "localhost", "Hostname of the Mesos master")
	masterPort = flag.Int("master_port", 5050, "Port of the Mesos master")

	ip string

	httpWaitGroup sync.WaitGroup

	events = make(chan *mesos_scheduler.Event)
)

func init() {
	// Get our local IP address.
	name, err := os.Hostname()
	if err != nil {
		log.Fatalf("Failed to get hostname: %+v", err)
	}

	addrs, err := net.LookupHost(name)
	if err != nil {
		log.Fatalf("Failed to get address for hostname %q: %+v", name, err)
	}

	log.Printf("using ip %q", addrs[0])
	ip = addrs[0]
}

// StatusUpdate is a wrapper around a Mesos Task Status Update.
type StatusUpdate struct {
	TaskId  string
	SlaveId string
	State   mesos.TaskState
	uuid    []byte
}

// EventChannels are the channels on which events are communicated to a framework.
type Driver struct {
	Offers        chan mesos.Offer
	Updates       chan StatusUpdate
	frameworkName string
	user          string
	frameworkId   mesos.FrameworkID
}

func NewDriver() *Driver {
	m := new(Driver)
	m.Offers = make(chan mesos.Offer)
	m.Updates = make(chan StatusUpdate)

	return m
}

// Register with a running master as the given user and framework name.
func (d *Driver) Register(user, name string) error {
	d.frameworkName = name
	d.user = user

	// Create the register message and send it.
	callType := mesos_scheduler.Call_REGISTER
	registerCall := &mesos_scheduler.Call{
		Type: &callType,
		FrameworkInfo: &mesos.FrameworkInfo{
			User: &d.user,
			Name: &d.frameworkName,
		},
	}

	httpWaitGroup.Wait()

	err := d.send(registerCall)
	if err != nil {
		return err
	}

	return nil
}

// Run starts listening for events from the Mesos master.
func (d *Driver) Run() error {
	for {
		log.Printf("waiting for mesos event...")
		event := <-events
		log.Printf("... got event type %+v", *event.Type)

		switch *event.Type {
		case mesos_scheduler.Event_REGISTERED:
			d.frameworkId = *event.Registered.FrameworkId
			log.Printf("registered %s:%s with id %q", d.user, d.frameworkName, *d.frameworkId.Value)
			//RegisteredHandler()
			break

		case mesos_scheduler.Event_OFFERS:
			for _, offer := range event.Offers.Offers {
				if *offer.FrameworkId.Value != *d.frameworkId.Value {
					log.Printf("Unexpected framework in offer: want %q, got %q", *d.frameworkId.Value, *offer.FrameworkId.Value)
				}
				d.Offers <- *offer
			}
			break

		case mesos_scheduler.Event_UPDATE:
			// TODO(dhamon): Pass through event.Update.Status.GetMessage()?
			d.Updates <- StatusUpdate{
				TaskId:  *event.Update.Status.TaskId.Value,
				SlaveId: *event.Update.Status.SlaveId.Value,
				State:   *event.Update.Status.State,
				uuid:    event.Update.Uuid,
			}
			break

		default:
			return fmt.Errorf("Unexpected event %+v", *event)
		}
	}
}

// SendAck send an acknowledgement of the given task status update.
func (d Driver) SendAck(update StatusUpdate) error {
	acknowledgeType := mesos_scheduler.Call_ACKNOWLEDGE
	acknowledgeCall := &mesos_scheduler.Call{
		FrameworkInfo: &mesos.FrameworkInfo{
			User: &d.user,
			Name: &d.frameworkName,
			Id:   &d.frameworkId,
		},
		Type: &acknowledgeType,
		Acknowledge: &mesos_scheduler.Call_Acknowledge{
			SlaveId: &mesos.SlaveID{
				Value: &update.SlaveId,
			},
			TaskId: &mesos.TaskID{
				Value: &update.TaskId,
			},
			Uuid: update.uuid,
		},
	}

	err := d.send(acknowledgeCall)
	if err != nil {
		return fmt.Errorf("failed to send acknowledgement: %+v", err)
	}

	return nil
}

// LaunchTask launches the given command as a task and consumes the offer.
func (d Driver) LaunchTask(offer mesos.Offer, id, command string) error {
	log.Printf("launching %s: %q", id, command)

	launchType := mesos_scheduler.Call_LAUNCH
	launchCall := &mesos_scheduler.Call{
		// TODO(dhamon): move setting of framework info to driver.send?
		FrameworkInfo: &mesos.FrameworkInfo{
			User: &d.user,
			Name: &d.frameworkName,
			Id:   &d.frameworkId,
		},
		Type: &launchType,
		Launch: &mesos_scheduler.Call_Launch{
			TaskInfos: []*mesos.TaskInfo{
				&mesos.TaskInfo{
					Name: &command,
					TaskId: &mesos.TaskID{
						Value: &id,
					},
					SlaveId:   offer.SlaveId,
					Resources: offer.Resources,
					Command: &mesos.CommandInfo{
						Value: &command,
					},
				},
			},
			OfferIds: []*mesos.OfferID{
				offer.Id,
			},
		},
	}

	err := d.send(launchCall)
	if err != nil {
		return err
	}

	return nil
}
