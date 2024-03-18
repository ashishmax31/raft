package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	labgob "github.com/ashishmax31/raft/encoder"
	"github.com/google/uuid"
)

const (
	DEBUG                = false
	leaderTickerDuration = time.Duration(200 * time.Millisecond)
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type VotedFor struct {
	Candidate string
	Term      int32
}

type RaftPeer interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool
}

type LogEntry struct {
	Content  interface{}
	Term     int32
	LogIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	candidateID      string
	mu               sync.RWMutex // Lock to protect shared access to this peer's state
	peers            []RaftPeer   // RPC end points of all peers
	persister        *Persister   // Object to hold this peer's persisted state
	me               int          // this peer's index into peers[]
	dead             int32        // set by Kill()
	currentTerm      int32
	votedFor         *VotedFor
	heartBeatChan    chan struct{}
	stopElectionChan chan struct{}
	logContents      []LogEntry
	commitIndex      int
	lastApplied      int
	lastLogIndex     int
	replicators      []Replicator
	currentLeader    RaftPeer
	killedChan       chan struct{}
	electionMgr      LeaderElectionManager
	applyCh          chan ApplyMsg
	debug            bool
	StateManager
	LogReplicationState
}

func (rf *Raft) Log(msg string, args ...any) {
	if rf.debug {
		prefix := fmt.Sprintf("[instance: %s, me: %d]: ", rf.candidateID, rf.me)
		line := fmt.Sprintf(msg, args...)
		fmt.Fprintln(os.Stdout, prefix+line)
	}
}

func (rf *Raft) LogUrgent(msg string, args ...any) {
	prefix := fmt.Sprintf("[instance: %s, me: %d]: ", rf.candidateID, rf.me)
	line := fmt.Sprintf(msg, args...)
	fmt.Fprintln(os.Stdout, prefix+line)
}

type LeaderElectionManager interface {
	RunElection(KilledChan chan struct{}, stopElectionChan chan struct{})
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.GetCurrentTerm()), rf.IsLeader()
}

func (rf *Raft) Persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	state := RaftNonVolatileState{
		VotedFor: rf.votedFor,
		Term:     rf.currentTerm,
		Log:      rf.logContents,
	}
	e.Encode(state)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state RaftNonVolatileState
	if d.Decode(&state) != nil {
		panic("failed to read raft state")
	} else {
		rf.currentTerm = state.Term
		rf.logContents = state.Log
		rf.votedFor = state.VotedFor
	}
}

func (rf *Raft) ApplyToStateMachine() {
	currentLastApplied := rf.lastApplied
	for {
		if rf.commitIndex > currentLastApplied {
			currentLastApplied = currentLastApplied + 1
			rf.Log("Applying to statemachine log at %d", currentLastApplied)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.logContents[currentLastApplied-1].Content,
				CommandIndex: currentLastApplied,
			}
		} else {
			break
		}
	}
	rf.lastApplied = currentLastApplied
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := rf.IsLeader()
	if !isLeader {
		return 0, 0, false
	}
	// Acquire rf's lock only after calling `rf.IsLeader()` as `rf.IsLeader()` internally
	// uses another lock so as to prevent deadlocks.
	rf.mu.Lock()
	rf.Log("got new cmd[%v] for leader log.", command)
	currentTerm := rf.currentTerm
	currentLastLogIndex := len(rf.logContents)
	rf.logContents = append(rf.logContents, LogEntry{
		Content:  command,
		Term:     currentTerm,
		LogIndex: currentLastLogIndex + 1,
	})
	rf.persist()
	rf.lastLogIndex = len(rf.logContents)
	currentLastLogIndex = rf.lastLogIndex
	rf.SetMatchIndex(rf.me, rf.lastLogIndex)
	for _, replicator := range rf.replicators {
		if replicator != nil {
			replicator.Replicate(currentLastLogIndex)
		}
	}
	rf.mu.Unlock()
	rf.Log("added new cmd[%v] to leader log.  Current leader last log index: %d", command, currentLastLogIndex)
	return currentLastLogIndex, int(currentTerm), true
}

func (rf *Raft) Kill() {
	rf.Log("killed me")
	atomic.StoreInt32(&rf.dead, 1)
	close(rf.killedChan)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []RaftPeer, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	id := uuid.NewString()
	rf := &Raft{
		candidateID:      id,
		heartBeatChan:    make(chan struct{}),
		stopElectionChan: make(chan struct{}),
		killedChan:       make(chan struct{}),
		applyCh:          applyCh,
		debug:            DEBUG,
	}

	stopLeaderChan := make(chan struct{})
	stopFollowerChan := make(chan struct{})
	rf.StateManager = NewStateManager(me, stopLeaderChan, rf.stopElectionChan, stopFollowerChan, rf.killedChan)
	rf.LogReplicationState = NewLogReplicationState(len(peers))
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	electionManager := NewElectionManager(rf)
	rf.electionMgr = electionManager

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.StartRaftManager(stopFollowerChan, stopLeaderChan)
	rf.TransitionToFollower("start")
	return rf
}

func (rf *Raft) StartRaftManager(stopFollowerChan, stopLeaderChan chan struct{}) {
	leaderChan := rf.LeaderChan()
	candidateChan := rf.CandidateChan()
	followerChan := rf.FollowerChan()
	for {
		select {
		case <-followerChan:
			go rf.StartFollowerWatchDog(stopFollowerChan)
			rf.Log("transitioned to follower")
		case <-leaderChan:
			go rf.StartLeaderRoutine(stopLeaderChan)
			rf.Log("transitioned to leader: term: %d", rf.GetCurrentTerm())
		case <-candidateChan:
			go rf.electionMgr.RunElection(rf.killedChan, rf.stopElectionChan)
			rf.Log("transitioned to candidate: term: %d", rf.GetCurrentTerm())
		}
	}
}

func (rf *Raft) intializeLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastLogIndex = len(rf.logContents)
	rf.ResetLogReplicationState()
	for i := range rf.peers {
		if i != rf.me {
			rf.SetNextIndex(i, rf.lastLogIndex+1)
		}
		if i == rf.me {
			rf.SetMatchIndex(i, rf.lastLogIndex)
		} else {
			rf.SetMatchIndex(i, 0)
		}
	}
	rf.replicators = make([]Replicator, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			rf.replicators[i] = NewReplicator(rf, i, rf.currentTerm)
		}
	}
}

func (rf *Raft) stopReplicators() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	for _, replicator := range rf.replicators {
		if replicator != nil {
			replicator.Stop()
		}
	}
}

func (rf *Raft) StartLeaderRoutine(stopChan chan struct{}) {
	rf.intializeLeaderState()
	defer rf.Log("no longer leader!!!![term: %d]", rf.GetCurrentTerm())
	stopHeartBeatsChan := make(chan struct{})
	defer rf.stopReplicators()
	defer close(stopHeartBeatsChan)
	currentTerm := rf.GetCurrentTerm()
	go rf.sendInitialHeartBeats(stopHeartBeatsChan, currentTerm)
	leaderTicker := time.NewTicker(leaderTickerDuration)
	defer leaderTicker.Stop()
	rf.Log("starting leader routine. term: %d", rf.GetCurrentTerm())
	rf.SendTransitionedToLeaderAck()
	for {
		select {
		case <-rf.killedChan:
			return
		case <-leaderTicker.C:
			go rf.SendHeartBeats(stopHeartBeatsChan, currentTerm)
			// This needs to be in a go routine since UpdateLeaderCommitIndex needs to acquire the rf mutex.
			// There can be a situation when from one of the raft rpcs like request vote where the lock is first acquired
			// and then from the rpc we try to stop the leader routine, but if the leader routine here is stuck trying to acquire
			// already acquired mutex in UpdateLeaderCommitIndex. So essentially this for-select loop becomes stuck.
			go rf.UpdateLeaderCommitIndex()
		case <-stopChan:
			rf.sendAck(stopChan)
			rf.ReleaseLeaderFlag()
			return
		}
	}
}

func (rf *Raft) sendInitialHeartBeats(stopChan chan struct{}, forTerm int32) {
	for i := range rf.peers {
		select {
		case <-rf.killedChan:
			return
		case <-stopChan:
			return
		default:
		}
		// Dont send the current commit index in the initial heartbeats.
		arg := AppendEntriesArgs{
			Term:         forTerm,
			LeaderID:     rf.me,
			LeaderCommit: 0,
		}
		reply := AppendEntriesReply{}
		if i != rf.me {
			go func(ind int) {
				res := rf.SendAppendEntries(ind, &arg, &reply)
				if res {
					if !reply.Success {
						select {
						case <-rf.killedChan:
							return
						case <-stopChan:
							return
						default:
						}
						if reply.Term > rf.GetCurrentTerm() {
							rf.Log("Send heartbeat failed for peer: %d: peer term: %d my term: %d", ind, reply.Term, rf.GetCurrentTerm())
							rf.TransitionToFollower(fmt.Sprintf("from send init heartBeat [%s]from leader with term: %d", rf.candidateID, rf.currentTerm))
							rf.SetTerm(reply.Term)
							return
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) SendHeartBeats(stopChan chan struct{}, forTerm int32) {
	for _, replicator := range rf.replicators {
		select {
		case <-stopChan:
			return
		case <-rf.killedChan:
			return
		default:
		}
		if replicator != nil {
			replicator.Replicate(rf.LastLogIndex())
		}
	}
}

func getElectionTimeoutDuration() time.Duration {
	rand.Seed(time.Now().UnixNano())
	randRange := rand.Intn(50) + rand.Intn(150) + 450
	return time.Millisecond * time.Duration(randRange)
}

func (rf *Raft) StartFollowerWatchDog(stopChan chan struct{}) {
	var mu sync.RWMutex
	timeoutDuration := getElectionTimeoutDuration()
	timeoutChan := make(chan struct{})
	exitedChan := make(chan struct{})
	defer close(exitedChan)
	start := time.Now()
	rf.SendTransitionedToFollowerAck()
	go func() {
		for {
			mu.RLock()
			if time.Since(start).Milliseconds() > timeoutDuration.Milliseconds() {
				mu.RUnlock()
				select {
				case <-exitedChan:
					close(timeoutChan)
					return
				case <-stopChan:
					return
				case <-rf.killedChan:
					return
				default:
					timeoutChan <- struct{}{}
					return
				}
			}
			mu.RUnlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	rf.Log("timeout duration: %s", timeoutDuration.String())
	for {
		select {
		case <-rf.heartBeatChan:
			mu.Lock()
			start = time.Now()
			mu.Unlock()
		case <-stopChan:
			rf.ReleaseFollowerFlag()
			rf.sendAck(stopChan)
			return
		case <-rf.killedChan:
			rf.ReleaseFollowerFlag()
			return
		case <-timeoutChan:
			rf.ReleaseFollowerFlag()
			rf.Log("election timeout")
			rf.TransitionToCandidate("transitioning to candidate")
			rf.Log("send to candidate chan")
			return
		}
	}
}
