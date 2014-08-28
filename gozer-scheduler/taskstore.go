package main

import (
	"fmt"
	"sync"

	"github.com/twitter/gozer/mesos"
	"github.com/twitter/gozer/gozer"
)

type Task struct {
	gozerTask *gozer.Task
	mesosTask *mesos.MesosTask
}

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

	if _, ok := t.tasks[task.gozerTask.Id]; ok {
		return fmt.Errorf("task Id %q already exists; addition ignored", task.gozerTask.Id)
	}

	task.gozerTask.State = gozer.TaskState_INIT
	task.mesosTask = &mesos.MesosTask{
		Id:      task.gozerTask.Id,
		Command: task.gozerTask.Command,
	}
	t.tasks[task.gozerTask.Id] = task
	log.Debug.Printf("TASK %q State * -> %s", task.gozerTask.Id, task.gozerTask.State)

	return nil
}

func (t *TaskStore) Update(taskId string, state gozer.TaskState) error {
	t.Lock()
	defer t.Unlock()

	task, ok := t.tasks[taskId]
	if !ok {
		return fmt.Errorf("task Id %q not found, update ignored", taskId)
	}

	log.Debug.Printf("TASK %q State %s -> %s", taskId, task.gozerTask.State, state)
	task.gozerTask.State = state

	if task.gozerTask.IsTerminal() {
		log.Info.Printf("Removing terminal task %q", taskId)
		delete(t.tasks, taskId)
		log.Debug.Printf("TASK %q removed", taskId)
	}

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

func (t *TaskStore) State(taskId string) (gozer.TaskState, error) {
	t.RLock()
	defer t.RUnlock()

	task, ok := t.tasks[taskId]
	if !ok {
		return "", fmt.Errorf("task Id %q not found", taskId)
	}

	return task.gozerTask.State, nil
}

func (t *TaskStore) MesosTask(taskId string) (*mesos.MesosTask, error) {
	t.RLock()
	defer t.RUnlock()

	task, ok := t.tasks[taskId]
	if !ok {
		return nil, fmt.Errorf("task Id %q not found", taskId)
	}

	return task.mesosTask, nil
}
