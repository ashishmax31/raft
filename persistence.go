package raft

type RaftNonVolatileState struct {
	VotedFor *VotedFor
	Term     int32
	Log      []LogEntry
}
