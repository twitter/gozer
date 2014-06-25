package mesos

import (
	"bytes"
	"fmt"
	"log"
	"net/http"

	"code.google.com/p/goprotobuf/proto"

	"mesos.pb"
	"messages.pb"
	"scheduler.pb"
)

func path(m *mesos_scheduler.Call) (string, error) {
	switch *m.Type {
	case mesos_scheduler.Call_REGISTER:
		return "mesos.internal.RegisterFrameworkMessage", nil
	case mesos_scheduler.Call_REQUEST:
		return "mesos.internal.ResourceRequestMessage", nil
	case mesos_scheduler.Call_LAUNCH:
		return "mesos.internal.LaunchTasksMessage", nil
	case mesos_scheduler.Call_ACKNOWLEDGE:
		return "mesos.internal.StatusUpdateAcknowledgementMessage", nil
	}
	return "", fmt.Errorf("unimplemented call type %q", *m.Type)
}

func callToMessage(m *mesos_scheduler.Call) (proto.Message, error) {
	log.Printf("converting from %+v", m)
	switch *m.Type {
	case mesos_scheduler.Call_REGISTER:
		return &mesos_internal.RegisterFrameworkMessage{
			Framework: m.FrameworkInfo,
		}, nil

	case mesos_scheduler.Call_REQUEST:
		return &mesos_internal.ResourceRequestMessage{
			FrameworkId: m.FrameworkInfo.Id,
			Requests: m.Request.Requests,
		}, nil

	case mesos_scheduler.Call_LAUNCH:
		filters := m.Launch.Filters
		if filters == nil {
			filters = &mesos.Filters {}
		}
		return &mesos_internal.LaunchTasksMessage{
			FrameworkId: m.FrameworkInfo.Id,
			Tasks: m.Launch.TaskInfos,
			OfferIds: m.Launch.OfferIds,
			Filters: filters,
		}, nil

	case mesos_scheduler.Call_ACKNOWLEDGE:
		return &mesos_internal.StatusUpdateAcknowledgementMessage{
			SlaveId: m.Acknowledge.SlaveId,
			FrameworkId: m.FrameworkInfo.Id,
			TaskId: m.Acknowledge.TaskId,
			Uuid: m.Acknowledge.Uuid,
		}, nil
	}

	return nil, fmt.Errorf("unimplemented call type %q", *m.Type)
}

func send(m *mesos_scheduler.Call) error {
	// TODO(dhamon): Remove this call when mesos listens for Call directly.
	msg, err := callToMessage(m)
	if err != nil {
		return fmt.Errorf("failed to convert Call %+v: %+v", m, err)
	}

	buffer, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal Message %+v: %+v", msg, err)
	}

	path, err := path(m)
	if err != nil {
		return fmt.Errorf("failed to get path for Call %+v: %+v", m, err)
	}

	registerUrl := "http://" + fmt.Sprintf("%s:%d/master", *master, *masterPort) + "/" + path
	log.Printf("sending %+v to %s", msg, registerUrl)

	client := &http.Client{}
	req, err := http.NewRequest("POST", registerUrl, bytes.NewReader(buffer))
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Content-type", "application/octet-stream")
	req.Header.Add("Libprocess-From", fmt.Sprintf("gozer@%s:%d", ip, port))

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to post register call to %s: %+v", registerUrl, err)
	}

	if resp != nil && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected response status. want %d got %d", http.StatusAccepted, resp.StatusCode)
	}

	return nil
}

