package gozer

import (
	"fmt"

	mesos_pb "github.com/twitter/gozer/proto/mesos.pb"
)

type TaskState string

const (
	TaskState_INIT     TaskState = "INIT"
	TaskState_STARTING TaskState = "STARTING"
	TaskState_RUNNING  TaskState = "RUNNING"
	TaskState_FINISHED TaskState = "FINISHED"
	TaskState_FAILED   TaskState = "FAILED"
	TaskState_KILLED   TaskState = "KILLED"
	TaskState_LOST     TaskState = "LOST"
)

var TaskStateMap = map[mesos_pb.TaskState]TaskState{
	mesos_pb.TaskState_TASK_STAGING:  TaskState_STARTING,
	mesos_pb.TaskState_TASK_STARTING: TaskState_STARTING,
	mesos_pb.TaskState_TASK_RUNNING:  TaskState_RUNNING,
	mesos_pb.TaskState_TASK_FINISHED: TaskState_FINISHED,
	mesos_pb.TaskState_TASK_FAILED:   TaskState_FAILED,
	mesos_pb.TaskState_TASK_KILLED:   TaskState_KILLED,
	mesos_pb.TaskState_TASK_LOST:     TaskState_LOST,
}

type Task struct {
	Id      string    `json:"id"`
	Command string    `json:"command"`
	State   TaskState `json:"state"`
	// TODO(dhamon): resource requirements
}

func (t Task) String() string {
	return fmt.Sprintf("%s: %q @ %s", t.Id, t.Command, t.State)
}

func (t Task) IsTerminal() bool {
	return t.State == TaskState_FAILED ||
		t.State == TaskState_FINISHED ||
		t.State == TaskState_KILLED ||
		t.State == TaskState_LOST
}
