package main

import (
	"fmt"
	"sync"

	"github.com/golang/glog"
	"github.com/twitter/gozer/mesos"
)

type TaskStore struct {
	sync.RWMutex
	tasks map[string]*Task
}

func NewTaskStore() *TaskStore {
	return &TaskStore{
		tasks: make(map[string]*Task),
	}
}

func (t *TaskStore) Add(task *Task) error {
	t.Lock()
	defer t.Unlock()

	if _, ok := t.tasks[task.Id]; ok {
		return fmt.Errorf("task id %q already exists; addition ignored", task.Id)
	}

	task.State = TaskState_INIT
	task.mesosTask = &mesos.MesosTask{
		Id:      task.Id,
		Command: task.Command,
	}
	t.tasks[task.Id] = task
	glog.V(1).Infof("gozer: TASK %q State * -> %s", task.Id, task.State)

	return nil
}

func (t *TaskStore) Update(taskId string, state TaskState) error {
	t.Lock()
	defer t.Unlock()

	task, ok := t.tasks[taskId]
	if !ok {
		return fmt.Errorf("task id %q not found, update ignored", taskId)
	}

	glog.V(1).Infof("gozer: TASK %q State %s -> %s", taskId, task.State, state)
	task.State = state

	return nil
}

func (t *TaskStore) Ids() []string {
	t.RLock()
	defer t.RUnlock()

	keys := make([]string, 0)
	for key := range t.tasks {
		keys = append(keys, key)
	}

	return keys
}

func (t *TaskStore) State(taskId string) (TaskState, error) {
	t.RLock()
	defer t.RUnlock()

	task, ok := t.tasks[taskId]
	if !ok {
		return TaskState_UNKNOWN, fmt.Errorf("task Id '%s' not found", taskId)
	}

	return task.State, nil
}

func (t *TaskStore) MesosTask(taskId string) (*mesos.MesosTask, error) {
	t.RLock()
	defer t.RUnlock()

	task, ok := t.tasks[taskId]
	if !ok {
		return nil, fmt.Errorf("task Id '%s' not found", taskId)
	}

	return task.mesosTask, nil
}
