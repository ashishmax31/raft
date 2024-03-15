package raft

import (
	"context"
	"sync"
)

const (
	REPLICATE_SUCCESS = iota
	REPLICATE_FAILURE
	REPLICATE_EXIT
	REPLICATE_ERR
)

type RaftLeader interface {
	LastLogIndex() int
	GetCurrentTerm() int32
	SendAppendEntry(ctx context.Context, peer, from int, to int, currentTerm int32) (int, *AppendEntryExtras)
	GetLastEntryWithTerm(term int32) *LogEntry
	UpdateLeaderCommitIndex()
	Log(msg string, args ...interface{})
	LogUrgent(msg string, args ...interface{})
	SendHeartBeat(term int32, toPeer int)
	LeaderID() int
	IsLeader() bool
	LogReplicationState
}

type logReplicationState struct {
	mu         sync.RWMutex
	peerSize   int
	nextIndex  []int
	matchIndex []int
}

type LogReplicationState interface {
	NextIndex(forPeer int) int
	MatchIndex(forPeer int) int
	SetMatchIndex(peerIndex int, value int)
	SetNextIndex(peerIndex int, value int)
	MatchIndexCopy() []int
	ResetLogReplicationState()
}

func NewLogReplicationState(peerSize int) LogReplicationState {
	return &logReplicationState{
		peerSize:   peerSize,
		nextIndex:  make([]int, peerSize),
		matchIndex: make([]int, peerSize),
	}
}

func (rf *logReplicationState) MatchIndexCopy() []int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	res := make([]int, 0)
	res = append(res, rf.matchIndex...)
	return res
}

func (rf *logReplicationState) ResetLogReplicationState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex = make([]int, rf.peerSize)
	rf.matchIndex = make([]int, rf.peerSize)
}

func (rf *logReplicationState) NextIndex(forPeer int) int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.nextIndex[forPeer]
}

func (rf *logReplicationState) MatchIndex(forPeer int) int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.matchIndex[forPeer]
}

func (rf *logReplicationState) SetMatchIndex(peerIndex int, value int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndex[peerIndex] = value
}

func (rf *logReplicationState) SetNextIndex(peerIndex int, value int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[peerIndex] = value
}

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
	sync.Mutex
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

func (r *replicator) Replicate(till int) {
	select {
	case r.channel <- till:
		return
	case <-r.stopChan:
		return
	}
}

func (r *replicator) startReplicatorLoop(currentTerm int32) {
	defer r.leader.Log("exiting replicater loop: %d", r.peerIndex)
	r.leader.Log("started replicator with term: %d", currentTerm)
	for {
		select {
		case currentLeaderLastLogIndex := <-r.channel:
			if currentLeaderLastLogIndex >= r.leader.NextIndex(r.peerIndex) {
				r.replicate(r.leader.NextIndex(r.peerIndex), currentLeaderLastLogIndex, currentTerm)
			} else if currentLeaderLastLogIndex != r.leader.MatchIndex(r.peerIndex) {
				r.replicate(currentLeaderLastLogIndex, currentLeaderLastLogIndex, currentTerm)
			} else {
				go r.leader.SendHeartBeat(currentTerm, r.peerIndex)
			}
		case <-r.stopChan:
			r.goRoutineManager.StopAll()
			return
		}
	}
}

func (r *replicator) replicate(fromIndex int, toIndex int, term int32) {
	r.Lock()
	defer r.Unlock()
	currentRoutineID := createReplicatorID(fromIndex, toIndex)
	if r.goRoutineManager.Exists(ID(currentRoutineID)) {
		return
	}
	ctxWithCancel, exitChan := r.goRoutineManager.AddRoutine(ID(currentRoutineID))
	go func() {
		defer close(exitChan)
		for {
			res := r.replicateF(ctxWithCancel, fromIndex, toIndex, term)
			switch res {
			case REPLICATE_EXIT:
				return
			case REPLICATE_SUCCESS:
				return
			}
		}
	}()
	// Stop all other in flight replicate routines with ID less than the current ID.
	r.goRoutineManager.Stop(func(id ID) bool {
		return id < ID(currentRoutineID)
	})
}

func (r *replicator) replicateF(ctx context.Context, fromIndex int, toIndex int, currentTerm int32) int {
	// When the stop channel is closed, we need to return from all the recursive calls.
	defer r.leader.Log("exiting replicate func. peer: %d", r.peerIndex)
	if !r.leader.IsLeader() || fromIndex == 0 {
		return REPLICATE_EXIT
	}
	r.leader.Log("replicating from %d to %d for peer: %d", fromIndex, toIndex, r.peerIndex)
	res, extraInfo := r.leader.SendAppendEntry(ctx, r.peerIndex, fromIndex, toIndex, currentTerm)
	switch {
	case res == REPLICATE_ERR:
		return REPLICATE_ERR
	case !r.leader.IsLeader():
		return REPLICATE_EXIT
	case res == REPLICATE_FAILURE:
		r.leader.Log("failed to replicate. Info from follower[%d]: %#v", r.peerIndex, extraInfo)
		if extraInfo.Xterm == -1 {
			if extraInfo.XLen == 0 {
				return r.replicateF(ctx, 1, toIndex, currentTerm)
			}
			return r.replicateF(ctx, extraInfo.XLen, toIndex, currentTerm)
		}
		lastLeaderEntryWithConflictingTerm := r.leader.GetLastEntryWithTerm(extraInfo.Xterm)
		if lastLeaderEntryWithConflictingTerm == nil {
			return r.replicateF(ctx, extraInfo.XIndex, toIndex, currentTerm)
		} else {
			return r.replicateF(ctx, lastLeaderEntryWithConflictingTerm.LogIndex, toIndex, currentTerm)
		}
	case res == REPLICATE_SUCCESS:
		r.leader.SetMatchIndex(r.peerIndex, toIndex)
		r.leader.SetNextIndex(r.peerIndex, toIndex+1)
		r.leader.UpdateLeaderCommitIndex()
	case res == REPLICATE_EXIT:
		return REPLICATE_EXIT
	}
	return REPLICATE_SUCCESS
}
