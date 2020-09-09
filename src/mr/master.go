package mr

import (
	`log`
	`sync`
	`time`
	"net"
	"os"
	"net/rpc"
	"net/http"
)


type Master struct {
	// Your definitions here.
	workKeepLiveMu		*sync.RWMutex
	workLatestKeepLive	map[uint32]*time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) WorkerKeepLive(args *WorkerKeepLiveArgs, reply *WorkerKeepLiveReply) error {
	m.workKeepLiveMu.Lock()
	defer m.workKeepLiveMu.Unlock()
	if args.WorkId == 0 {
		workerCnt := len(m.workLatestKeepLive)
		reply.ConfirmedWorkId = uint32(workerCnt) + 1
		m.workLatestKeepLive[reply.ConfirmedWorkId] = args.CreateTime
	} else if args.WorkId != 0 {
		m.workLatestKeepLive[args.WorkId] = args.CreateTime
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	// 这里会注册所有Master内的方法
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.workLatestKeepLive = make(map[uint32]*time.Time, 0)
	m.workKeepLiveMu = &sync.RWMutex{}
	// Your code here.

	go func() {
		t := time.NewTicker(1*time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				m.workKeepLiveMu.RLock()
				log.Println(m.workLatestKeepLive)
				m.workKeepLiveMu.RUnlock()
			}
		}
	}()
	m.server()
	return &m
}
