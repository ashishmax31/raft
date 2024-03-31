package raft

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

const (
	BatchSize = 5000
)

var _ RaftLeader = (*Raft)(nil)

func (rf *Raft) LeaderID() int {
	return rf.me
}

func (rf *Raft) SendHeartBeat(forTerm int32, toPeer int) {
	arg := AppendEntriesArgs{
		Term:         forTerm,
		LeaderID:     rf.me,
		LeaderCommit: int(rf.GetCommitIndex()),
	}
	reply := AppendEntriesReply{}
	res := rf.SendAppendEntries(toPeer, &arg, &reply)
	if res {
		if !reply.Success && reply.Term > rf.GetCurrentTerm() {
			if rf.IsLeader() {
				rf.TransitionToFollower(fmt.Sprintf("from send heart beat [%s]from leader with term: %d", rf.candidateID, forTerm))
			}
			rf.Log("Send heartbeat failed for peer: %d: peer term: %d my term: %d", toPeer, reply.Term, forTerm)
			rf.SetTerm(reply.Term)
		}
	} else {
		rf.Log("send heartbeat failed for peer: %d", toPeer)
	}
}

func (rf *Raft) SendAppendEntry(ctx context.Context, peer int, from int, to int, currentTerm int32) (ReplicationStatus, *AppendEntryExtras) {
	i, j := from, from+BatchSize
	for ; i <= to && j <= to; i, j = j, j+BatchSize {
		res, extra := rf.sendAppendEntry(ctx, peer, i, j, currentTerm)
		if res != REPLICATE_SUCCESS {
			return res, extra
		}
	}
	return rf.sendAppendEntry(ctx, peer, i, to, currentTerm)
}

func (rf *Raft) sendAppendEntry(ctx context.Context, peer int, from int, to int, currentTerm int32) (ReplicationStatus, *AppendEntryExtras) {
	replyChan := make(chan bool)
	reply := &AppendEntriesReply{}
	go func() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		res := rf.SendAppendEntries(peer,
			&AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     rf.me,
				LeaderCommit: rf.GetCommitIndex(),
				PrevLogIndex: from - 1,
				PrevLogTerm:  int32(rf.GetPrevLogTerm(from)),
				Entries:      rf.GetLogEntries(from, to),
			},
			reply,
		)
		select {
		case <-ctx.Done():
			return
		case replyChan <- res:
		}
	}()
	select {
	case rpcRes := <-replyChan:
		switch {
		case !rpcRes:
			return REPLICATE_ERR, nil
		case !reply.Success:
			if reply.Term > rf.GetCurrentTerm() {
				rf.Log(
					"sendAppendEntry failed for peer: %d, got a reply with higher term, %d. leader term: %d . Replicating index range: %d-%d",
					peer,
					reply.Term,
					rf.GetCurrentTerm(),
					from,
					to,
				)
				rf.TransitionToFollower(fmt.Sprintf("from send append entry [%s]from leader with term: %d", rf.candidateID, rf.GetCurrentTerm()))
				rf.SetTerm(reply.Term)
				return REPLICATE_EXIT, nil
			}
			return REPLICATE_FAILURE, &reply.ExtraInfo
		case reply.Success:
			return REPLICATE_SUCCESS, nil
		default:
			return REPLICATE_ERR, nil
		}
	case <-ctx.Done():
		return REPLICATE_EXIT, nil
	}
}

func (rf *Raft) GetPrevLogTerm(leaderLogIndex int) int32 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if leaderLogIndex == 1 {
		return 0
	}
	return rf.logContents[leaderLogIndex-2].Term
}

func (rf *Raft) GetLogEntries(from, to int) []LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	res := make([]LogEntry, 0)
	res = append(res, rf.logContents[from-1:to]...)
	return res
}

func (rf *Raft) UpdateLeaderCommitIndex() {
	// rf.Log("current leader log: %#v", rf.logContents)
	// rf.Log("leader match index: %#v", rf.matchIndex)
	if rf.IsLeader() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		currentTerm := rf.currentTerm
		matchIndexSlice := rf.MatchIndexCopy()
		sort.Ints(matchIndexSlice)
		middleElement := len(matchIndexSlice) / 2
		candidateIndex := matchIndexSlice[middleElement]
		for i := candidateIndex; i > rf.commitIndex; i = i - 1 {
			if rf.logContents[i-1].Term == currentTerm {
				rf.commitIndex = i
				rf.ApplyToStateMachine()
				rf.Log("successfully updated leader commit index to %d and applied to state machine", rf.commitIndex)
				return
			}
		}
	}
}

func replicatedToAllNodes(ind int, matchIndexSlice []int) bool {
	for _, matchInd := range matchIndexSlice {
		if ind < matchInd {
			return false
		}
	}
	return true
}

// logReplicationState stores the leaders log replication state
// info like nextIndex, matchIndex.
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
	if value > rf.matchIndex[peerIndex] {
		rf.matchIndex[peerIndex] = value
	}
}

func (rf *logReplicationState) SetNextIndex(peerIndex int, value int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if value > rf.nextIndex[peerIndex] {
		rf.nextIndex[peerIndex] = value
	}
}
