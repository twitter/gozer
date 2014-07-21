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

	frameworkName  string
	registeredUser string
	frameworkId    mesos.FrameworkID

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

type StatusUpdate struct {
	TaskId	string
	SlaveId	string
	State	mesos.TaskState
	uuid	[]byte
}

type EventChannels struct {
	Offers chan mesos.Offer
	Updates chan StatusUpdate
}

func NewEventChannels() *EventChannels {
	m := new(EventChannels)
	m.Offers = make(chan mesos.Offer)
	m.Updates = make(chan StatusUpdate)

	return m
}

// Register with a running master.
func Register(user, name string) error {
	frameworkName = name
	registeredUser = user

	// Create the register message and send it.
	callType := mesos_scheduler.Call_REGISTER
	registerCall := &mesos_scheduler.Call{
		Type: &callType,
		FrameworkInfo: &mesos.FrameworkInfo{
			User: &user,
			Name: &frameworkName,
		},
	}

	httpWaitGroup.Wait()

	err := send(registerCall)
	if err != nil {
		return err
	}

	return nil
}

// Event loop
func (eventChannels *EventChannels) Run() error {
	for {
		log.Printf("waiting for mesos event...")
		event := <-events
		log.Printf("... got event type %+v", *event.Type)

		switch (*event.Type) {
		case mesos_scheduler.Event_REGISTERED:
			frameworkId = *event.Registered.FrameworkId
			log.Printf("registered %s:%s with id %q", registeredUser, frameworkName, *frameworkId.Value)
			//RegisteredHandler()
			break

		case mesos_scheduler.Event_OFFERS:
			for _, offer := range event.Offers.Offers {
				if *offer.FrameworkId.Value != *frameworkId.Value {
					log.Printf("Unexpected framework in offer: want %q, got %q", *frameworkId.Value, *offer.FrameworkId.Value)
				}
				eventChannels.Offers <- *offer
			}
			break

		case mesos_scheduler.Event_UPDATE:
			// TODO(dhamon): Pass through event.Update.Status.GetMessage()?
			eventChannels.Updates <- StatusUpdate{
				TaskId: *event.Update.Status.TaskId.Value,
				SlaveId: *event.Update.Status.SlaveId.Value,
				State: *event.Update.Status.State,
				uuid: event.Update.Uuid,
			}
			break

		default:
			return fmt.Errorf("Unexpected event %+v", *event)
		}
	}
}

// Send an acknowledgement of the status update for the given task id.
func SendAck(update StatusUpdate) error {
	acknowledgeType := mesos_scheduler.Call_ACKNOWLEDGE
	acknowledgeCall := &mesos_scheduler.Call{
		FrameworkInfo: &mesos.FrameworkInfo{
			User: &registeredUser,
			Name: &frameworkName,
			Id:   &frameworkId,
		},
		Type: &acknowledgeType,
		Acknowledge: &mesos_scheduler.Call_Acknowledge{
			SlaveId: &mesos.SlaveID{
				Value: &update.SlaveId,
			},
			TaskId:  &mesos.TaskID{
				Value: &update.TaskId,
			},
			Uuid:    update.uuid,
		},
	}

	err := send(acknowledgeCall)
	if err != nil {
		return fmt.Errorf("failed to send acknowledgement: %+v", err)
	}

	return nil
}

// Launch the given command as a task and consume the offer.
func LaunchTask(offer mesos.Offer, id, command string) error {
	log.Printf("launching %s: %q", id, command)

	launchType := mesos_scheduler.Call_LAUNCH
	launchCall := &mesos_scheduler.Call{
		FrameworkInfo: &mesos.FrameworkInfo{
			User: &registeredUser,
			Name: &frameworkName,
			Id:   &frameworkId,
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

	err := send(launchCall)
	if err != nil {
		return err
	}

	return nil
}
