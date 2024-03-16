package raft

import (
	"context"
	"fmt"
	"time"
)

const (
	ELECTION_EXIT = iota
	ELECTION_LOST
	ELECTION_WON
	ELECTION_TIMEOUT
)

type RaftElectionInstance interface {
	GetCurrentTerm() int32
	Peers() []RaftPeer
	Me() int
	GetCandidateID() string
	IncrementTerm()
	SendRequestVote(int, *RequestVoteArgs, *RequestVoteReply) bool
	VoteForSelf()
	Log(msg string, args ...any)
	LastLogIndex() int
	LastLogTerm() int32
	LogLength() int
	SetTerm(int32) int32
	SendElectionRunningAck()
	ReleaseElectionRunningFlag()
	TransitionToLeader(term int)
	TransitionToFollower(info string)
}

type leaderElectionManager struct {
	raftInstance RaftElectionInstance
}

func NewElectionManager(instance RaftElectionInstance) *leaderElectionManager {
	return &leaderElectionManager{
		raftInstance: instance,
	}
}

func (mgr *leaderElectionManager) RunElection(KilledChan chan struct{}, stopElectionChan chan struct{}) {
	mgr.raftInstance.SendElectionRunningAck()
	electionResult := mgr.runElection(KilledChan, stopElectionChan)
	mgr.raftInstance.ReleaseElectionRunningFlag()
	switch electionResult {
	case ELECTION_WON:
		mgr.raftInstance.TransitionToLeader(int(mgr.raftInstance.GetCurrentTerm()))
		return
	case ELECTION_EXIT:
		// Just exit, dont tranistion to any particular state.
		// What this means is that the peer is already tranistioning to a follower or the peer has been killed.
		return
	case ELECTION_TIMEOUT:
		mgr.raftInstance.TransitionToFollower(fmt.Sprintf("from election timeout [%s]from candidate with term: %d", mgr.raftInstance.GetCandidateID(), mgr.raftInstance.GetCurrentTerm()))
		return
	case ELECTION_LOST:
		mgr.raftInstance.TransitionToFollower(fmt.Sprintf("from election lost [%s]from candidate with term: %d", mgr.raftInstance.GetCandidateID(), mgr.raftInstance.GetCurrentTerm()))
		return
	}
}

func (mgr *leaderElectionManager) runElection(stopElectionChan chan struct{}, killedChan chan struct{}) int {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	timeoutDuration := getElectionTimeoutDuration()
	timer := time.NewTimer(timeoutDuration)
	defer func() {
		timer.Stop()
	}()
	peers := mgr.raftInstance.Peers()
	// Increment current term for this instance.
	mgr.raftInstance.IncrementTerm()
	mgr.raftInstance.VoteForSelf()
	mgr.raftInstance.Log("candidate term: %d", mgr.raftInstance.GetCurrentTerm())

	replyChan := make(chan RequestVoteReply, len(peers)-1)

	// Voting
	arg := RequestVoteArgs{
		Term:         mgr.raftInstance.GetCurrentTerm(),
		CandidateID:  mgr.raftInstance.GetCandidateID(),
		LastLogIndex: int32(mgr.raftInstance.LastLogIndex()),
		LastLogTerm:  mgr.raftInstance.LastLogTerm(),
	}

	for i := range peers {
		if i != mgr.raftInstance.Me() {
			// Send out request vote rpcs to other peers other than self.
			reply := RequestVoteReply{}
			arg := arg
			mgr.asynSendRequestVote(ctx, i, arg, reply, replyChan)
		}
	}

	// Already has one vote since the candidate voted for itself.
	votesReceived := 1
	for {
		if votesReceived > len(peers)/2 {
			mgr.raftInstance.Log("won election: received: %d votes. my log length %d", votesReceived, mgr.raftInstance.LogLength())
			return ELECTION_WON
		}
		select {
		case resp := <-replyChan:
			if resp.Granted {
				votesReceived += 1
			} else {
				if resp.Term > mgr.raftInstance.GetCurrentTerm() {
					mgr.raftInstance.SetTerm(resp.Term)
				}
			}
			continue
		case <-timer.C:
			mgr.raftInstance.Log("election running timeout")
			return ELECTION_TIMEOUT
		case <-stopElectionChan:
			mgr.raftInstance.Log("asked to stop election")
			return ELECTION_EXIT
		case <-killedChan:
			return ELECTION_EXIT
		}
	}
}

func (mgr *leaderElectionManager) asynSendRequestVote(ctx context.Context, server int, arg RequestVoteArgs, reply RequestVoteReply, replyChan chan RequestVoteReply) {
	go func(arg RequestVoteArgs, reply RequestVoteReply, replyChan chan RequestVoteReply) {
		mgr.raftInstance.SendRequestVote(server, &arg, &reply)
		select {
		case <-ctx.Done():
			return
		default:
			replyChan <- reply
		}
	}(arg, reply, replyChan)
}
