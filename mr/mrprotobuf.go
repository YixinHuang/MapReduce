package mr

import (
	"fmt"
	"log"
	"time"
)

type MRPhaseType int

const (
	MapPhase    MRPhaseType = 0
	ReducePhase MRPhaseType = 1
	FinishPhase MRPhaseType = 2
)

var Phaseype_name = [...]string{
	"Map Phase",
	"Reduce Phase",
	"Finish Phase",
}

func (x MRPhaseType) String() string {
	return Phaseype_name[x]
}

type TaskType int

const (
	Map    TaskType = 0
	Reduce TaskType = 1
)

var TaskType_name = [...]string{
	"Map",
	"Reduce",
}

func (x TaskType) String() string {
	return TaskType_name[x]
}

type TaskStatus int

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Completed  TaskStatus = 2
)

var TaskStatus_name = [...]string{
	"Idle",
	"InProgress",
	"Completed",
}

func (x TaskStatus) String() string {
	return TaskStatus_name[x]
}

type Task struct {
	Num    int        //task number
	Type   TaskType   //task cmd :map or reduce
	Status TaskStatus //task state:idle, in-progress, or completed
	FName  string
	//task worker machine
	//stores the locations and sizes of the R intermediate ﬁle regions produced by the map
}

func (m *Task) Reset() { *m = Task{Num: -1} }
func (m *Task) String() string {
	return fmt.Sprintf("Task: Num:=[%d] Type:=[%s] Status:=[%s] FName:=[%s]", m.Num, m.Type.String(), m.Status.String(), m.FName)
}

//replytask.FName = "FinishPhase"
func (m *Task) IsFinished() bool {
	if m.FName == "FinishPhase" && m.Num == -1 {
		return true
	}
	return false
}

func (m *Task) ResetStatus(master *Master) {
	DPrintf("[ResetStatus@mrprotobuf.go] QueryTask Entry")
	time.Sleep(5 * time.Second) //observe no output
	master.mu.Lock()
	if m.Status == InProgress {
		log.Printf("ResetStatus: Task [%s] \n", m.String())
		DPrintf("[ResetStatus@mrprotobuf.go] Task [%s] \n", m.String())
		m.Status = Idle
	}
	master.mu.Unlock()
	DPrintf("[ResetStatus@mrprotobuf.go] QueryTask Entry")
}
