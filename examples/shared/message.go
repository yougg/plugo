package shared

//go:generate msgp

type RequestPayload struct {
	TaskName   string   `msg:"task_name" json:"task_name"`
	TaskDetail TaskInfo `msg:"task_detail" json:"task_detail"`
}

type TaskInfo struct {
	Step    int    `msg:"step" json:"step"`
	Options string `msg:"options" json:"options"`
}

type ResponsePayload struct {
	Code   int        `msg:"code" json:"code"`
	Result TaskResult `msg:"result" json:"result"`
}

type TaskResult struct {
	Message string      `msg:"message" json:"message"`
	Extra   ExtraResult `msg:"extra" json:"extra"`
}

type ExtraResult struct {
	NodeID string `msg:"node_id" json:"node_id"`
	Status string `msg:"status" json:"status"`
}

// IPCReqMessage is the unified request message contract for communication from host to different plugins.
type IPCReqMessage struct {
	ID      uint32         `msg:"id" json:"id"`           // Unique request ID used for concurrent callback multiplexing
	Command string         `msg:"command" json:"command"` // Command instruction to execute
	Payload RequestPayload `msg:"payload" json:"payload"` // Nested business payload data
}

// IPCRespMessage is the unified response message contract for communication from plugins to host.
type IPCRespMessage struct {
	ID      uint32          `msg:"id" json:"id"`           // Unique request ID used for concurrent callback multiplexing
	Command string          `msg:"command" json:"command"` // Command instruction to execute
	Payload ResponsePayload `msg:"payload" json:"payload"` // Nested business payload data
}
