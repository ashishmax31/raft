package raft

import (
	"context"
)

// ReplicationStatus represents the status of a replication operation.
type ReplicationStatus int

const (
	REPLICATE_SUCCESS ReplicationStatus = iota
	REPLICATE_FAILURE
	REPLICATE_EXIT
	REPLICATE_ERR
)

type RaftLeader interface {
	LastLogIndex() int
	GetCurrentTerm() int32
	SendAppendEntry(ctx context.Context, peer, from int, to int, currentTerm int32) (ReplicationStatus, *AppendEntryExtras)
	GetLastEntryWithTerm(term int32) *LogEntry
	UpdateLeaderCommitIndex()
	Log(msg string, args ...interface{})
	LogUrgent(msg string, args ...interface{})
	SendHeartBeat(term int32, toPeer int)
	LeaderID() int
	IsLeader() bool
	LogReplicationState
}

// Replicator defines methods for replicating logs to followers and stopping replication.
type Replicator interface {
	Replicate(tillIndex int)
	Stop()
}

type replicator struct {
	peerIndex        int
	channel          chan int
	stopChan         chan struct{}
	leader           RaftLeader
	goRoutineManager GoRoutineManager
}

func NewReplicator(leader RaftLeader, forPeer int, currentTerm int32) Replicator {
	r := &replicator{
		leader:           leader,
		peerIndex:        forPeer,
		channel:          make(chan int),
		stopChan:         make(chan struct{}),
		goRoutineManager: NewGoRoutineManager(),
	}
	r.leader.Log("started replicator for peer: %d", forPeer)
	go r.startReplicatorLoop(currentTerm)
	return r
}

func (r *replicator) Stop() {
	close(r.stopChan)
}

// Replicate initiates replication of logs up to a specified index.
func (r *replicator) Replicate(till int) {
	select {
	case <-r.stopChan:
		return
	default:
		r.channel <- till
	}
}

// Replicator main loop.
func (r *replicator) startReplicatorLoop(currentTerm int32) {
	defer r.leader.Log("exiting replicater loop: %d", r.peerIndex)
	r.leader.Log("started replicator with term: %d", currentTerm)
	for {
		select {
		case currentLeaderLastLogIndex := <-r.channel:
			r.handleReplicationRequest(currentLeaderLastLogIndex, currentTerm)
		case <-r.stopChan:
			r.goRoutineManager.StopAll()
			return
		}
	}
}

// handleReplicationRequest handles replication requests from the leader.
// It sends out heartbeats to peers if the log is already uptodate.
func (r *replicator) handleReplicationRequest(currentLeaderLastLogIndex int, currentTerm int32) {
	switch {
	case currentLeaderLastLogIndex >= r.leader.NextIndex(r.peerIndex):
		r.replicateLogs(r.leader.NextIndex(r.peerIndex), currentLeaderLastLogIndex, currentTerm)
	case currentLeaderLastLogIndex != r.leader.MatchIndex(r.peerIndex):
		r.replicateLogs(currentLeaderLastLogIndex, currentLeaderLastLogIndex, currentTerm)
	default:
		go r.leader.SendHeartBeat(currentTerm, r.peerIndex)
	}
}

func (r *replicator) replicateLogs(fromIndex int, toIndex int, term int32) {
	currentRoutineID := createReplicatorID(fromIndex, toIndex)
	if r.goRoutineManager.Exists(ID(currentRoutineID)) {
		return
	}
	ctxWithCancel, exitChan := r.goRoutineManager.AddRoutine(ID(currentRoutineID))
	go func() {
		defer close(exitChan)
		for {
			res := r.replicateLogsHelper(ctxWithCancel, fromIndex, toIndex, term)
			switch res {
			case REPLICATE_EXIT:
				return
			case REPLICATE_SUCCESS:
				return
			}
		}
	}()
	// Stop all other in flight replicate routines with ID less than the current ID.
	r.stopPreviousReplicationRoutines(ID(currentRoutineID))
}

func (r *replicator) replicateLogsHelper(ctx context.Context, fromIndex int, toIndex int, currentTerm int32) ReplicationStatus {
	defer r.leader.Log("exiting replicate func. peer: %d", r.peerIndex)
	if !r.leader.IsLeader() || fromIndex == 0 {
		return REPLICATE_EXIT
	}
	r.leader.Log("replicating from %d to %d for peer: %d", fromIndex, toIndex, r.peerIndex)
	res, extraInfo := r.leader.SendAppendEntry(ctx, r.peerIndex, fromIndex, toIndex, currentTerm)
	switch {
	case !r.leader.IsLeader():
		return REPLICATE_EXIT
	case res == REPLICATE_FAILURE:
		return r.handleReplicationFailure(ctx, extraInfo, toIndex, currentTerm)
	case res == REPLICATE_SUCCESS:
		r.updateLeaderReplicationIndexes(toIndex)
		return REPLICATE_SUCCESS
	}
	return res
}

// handleReplicationFailure handles replication failures.
func (r *replicator) handleReplicationFailure(ctx context.Context, extraInfo *AppendEntryExtras, toIndex int, currentTerm int32) ReplicationStatus {
	r.leader.Log("failed to replicate. Info from follower[%d]: %#v", r.peerIndex, extraInfo)
	if extraInfo.Xterm == -1 {
		if extraInfo.XLen == 0 {
			return r.replicateLogsHelper(ctx, 1, toIndex, currentTerm)
		}
		return r.replicateLogsHelper(ctx, extraInfo.XLen, toIndex, currentTerm)
	}
	lastLeaderEntryWithConflictingTerm := r.leader.GetLastEntryWithTerm(extraInfo.Xterm)
	if lastLeaderEntryWithConflictingTerm == nil {
		return r.replicateLogsHelper(ctx, extraInfo.XIndex, toIndex, currentTerm)
	}
	return r.replicateLogsHelper(ctx, lastLeaderEntryWithConflictingTerm.LogIndex, toIndex, currentTerm)
}

// stopPreviousReplicationRoutines stops all previous replication routines.
func (r *replicator) stopPreviousReplicationRoutines(currentRoutineID ID) {
	r.goRoutineManager.Stop(func(id ID) bool {
		return id < currentRoutineID
	})
}

// updateReplicationIndexes updates the match and next indexes.
func (r *replicator) updateLeaderReplicationIndexes(matchIndex int) {
	r.leader.SetMatchIndex(r.peerIndex, matchIndex)
	r.leader.SetNextIndex(r.peerIndex, matchIndex+1)
	r.leader.UpdateLeaderCommitIndex()
}
