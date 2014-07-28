package mesos

import (
	"fmt"

	"github.com/golang/glog"
	"github.com/twitter/gozer/proto/mesos.pb"
	"github.com/twitter/gozer/proto/scheduler.pb"
)

func (d *Driver) eventDispatch(event *mesos_scheduler.Event) error {
	switch *event.Type {
	case mesos_scheduler.Event_REGISTERED:
		glog.V(1).Info("Event REGISTERED:", event)

	case mesos_scheduler.Event_REREGISTERED:
		glog.V(1).Info("Event REREGISTERED:", event)

	case mesos_scheduler.Event_OFFERS:
		for _, offer := range event.Offers.Offers {
			if *offer.FrameworkId.Value != *d.frameworkId.Value {
				glog.Warningf("driver: unexpected framework in offer: want %q, got %q",
					*d.frameworkId.Value, *offer.FrameworkId.Value)
				continue
			}

			if len(d.Offers) < cap(d.Offers) {
				d.Offers <- offer
			} else {
				// TODO(weingart): how to ignore/return offer?
				glog.Warningf("driver: ignoring offer that we have no capacity for: %+v", offer)
			}
		}

	case mesos_scheduler.Event_RESCIND:
		glog.V(1).Info("Event RESCIND:", event)

	case mesos_scheduler.Event_UPDATE:
		glog.V(1).Info("Event UPDATE:", event)

		switch *event.Update.Status.State {
		case mesos.TaskState_TASK_STAGING,
			mesos.TaskState_TASK_STARTING,
			mesos.TaskState_TASK_RUNNING,
			mesos.TaskState_TASK_FINISHED,
			mesos.TaskState_TASK_FAILED,
			mesos.TaskState_TASK_KILLED,
			mesos.TaskState_TASK_LOST:

			d.Updates <- &TaskStateUpdate{
				TaskId:  event.Update.Status.GetTaskId().GetValue(),
				SlaveId: event.Update.Status.GetSlaveId().GetValue(),
				State:   event.Update.Status.GetState(),
				uuid:    event.Update.GetUuid(),
				driver:  d,
			}
		default:
			glog.Warning("driver: unknown Event_UPDATE:", event)
		}

	case mesos_scheduler.Event_MESSAGE:
		glog.V(1).Info("Event MESSAGE:", event)

	case mesos_scheduler.Event_FAILURE:
		glog.V(1).Info("Event FAILURE:", event)

	case mesos_scheduler.Event_ERROR:
		glog.V(1).Info("Event ERROR:", event)

	default:
		glog.Warning("driver: unexpected Event:", event)
		return fmt.Errorf("unexpected event type: %q", event.Type)
	}

	return nil
}
