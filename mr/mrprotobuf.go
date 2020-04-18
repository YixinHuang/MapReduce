package mr

import "fmt"

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

func (m *Task) Reset() { *m = Task{} }
func (m *Task) String() string {
	return fmt.Sprintf("Task: Num:=[%d] Type:=[%s] Status:=[%s] FName:=[%s]", m.Num, m.Type.String(), m.Status.String(), m.FName)
}
