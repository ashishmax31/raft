package raft

import "fmt"

type AppendEntriesArgs struct {
	Term         int32
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int32
	Success   bool
	ExtraInfo AppendEntryExtras
}

type RequestVoteArgs struct {
	Term         int32
	CandidateID  string
	LastLogIndex int32
	LastLogTerm  int32
}

type RequestVoteReply struct {
	Granted bool
	Term    int32
}

type AppendEntryExtras struct {
	Xterm  int32
	XIndex int
	XLen   int
}

// Server implemenentations

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	var resp RequestVoteReply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Log("got vote request from %s for term %d", args.CandidateID, args.Term)
	if (args.Term >= rf.currentTerm) && (rf.votedFor == nil || args.Term > rf.votedFor.Term) && rf.candidateUptodate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = &VotedFor{
			Candidate: args.CandidateID,
			Term:      args.Term,
		}
		rf.persist()
		if rf.IsLeader() {
			rf.Log("stepping down as leader from request vote")
			rf.SafeTransitionWithMutex(rf.TransitionToFollower, fmt.Sprintf("from request vote [%s]from leader with term: %d", rf.candidateID, rf.currentTerm))
		} else if rf.IsFollower() {
			rf.blockingNotifyWhile(rf.heartBeatChan, "sending heartbeat from request vote", rf.IsFollower)
		}
		resp = RequestVoteReply{
			Granted: true,
			Term:    args.Term,
		}

		*reply = resp
		rf.Log("accepted request vote from %s with term %d. My term: %d", args.CandidateID, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		if rf.IsLeader() {
			rf.Log("stepping down as leader from request vote")
			rf.SafeTransitionWithMutex(rf.TransitionToFollower, fmt.Sprintf("from request vote [%s]from leader with term: %d", rf.candidateID, rf.currentTerm))
		}
	}
	rf.Log("rejected request vote from %s for term %d. updated term: %d", args.CandidateID, args.Term, rf.currentTerm)
	resp = RequestVoteReply{
		Granted: false,
		Term:    rf.currentTerm,
	}
	*reply = resp
}

func (rf *Raft) candidateUptodate(candidateLastLogIndex int32, candidateLastLogTerm int32) bool {
	serverLogLen := int32(len(rf.logContents))
	if serverLogLen == 0 {
		return true
	}
	serverLastLogTerm := rf.logContents[serverLogLen-1].Term
	switch {
	case candidateLastLogTerm > serverLastLogTerm:
		return true
	case candidateLastLogTerm == serverLastLogTerm:
		return candidateLastLogIndex >= serverLogLen
	default:
		return false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.acknowledgeHeartBeat(args)
		if len(args.Entries) == 0 {
			rf.updateCommitIndex(args.LeaderCommit)
			// If its a heartbeat only append entry rpc call.
			*reply = AppendEntriesReply{
				Success: true,
				Term:    args.Term,
			}
			return
		}
		success, logChanged, extraInfo := rf.appendToLog(args)
		if logChanged {
			rf.persist()
		}
		if success {
			*reply = AppendEntriesReply{
				Success: true,
				Term:    args.Term,
			}
			rf.updateCommitIndex(min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
		} else {
			*reply = AppendEntriesReply{
				Success:   false,
				Term:      args.Term,
				ExtraInfo: *extraInfo,
			}
		}
		return
	}
	*reply = AppendEntriesReply{
		Success: false,
		Term:    rf.currentTerm,
	}
}

func (rf *Raft) appendToLog(args *AppendEntriesArgs) (bool, bool, *AppendEntryExtras) {
	lastEntryFromLeader := args.Entries[len(args.Entries)-1]
	currentLastLogIndex := len(rf.logContents)
	currentLogLen := currentLastLogIndex
	extraInfo := &AppendEntryExtras{
		Xterm:  -1,
		XIndex: -1,
		XLen:   -1,
	}
	rf.Log("peer Log: %#v: received args: %#v", rf.logContents, args)
	// If this log entry is the first log entry in the leaders log.
	if args.PrevLogIndex == 0 {
		defer rf.Log("appended to follower[%d] log. Current log size: %d", rf.me, len(rf.logContents))
		// If this peers log is empty.
		if currentLogLen == 0 {
			rf.logContents = append(rf.logContents, args.Entries...)
			return true, true, nil
		}

		// Only truncate the logs if the incoming entries modified the servers log.
		// Otherwise and out of order request can wipe out legit entries from the servers log.
		if logModified := rf.InsertEntries(currentLogLen, args); logModified {
			rf.logContents = rf.logContents[:lastEntryFromLeader.LogIndex]
			return true, true, nil
		}
		return true, false, nil
	}

	// Sanity check. The log is empty but the incoming leaders log entry is not the first.
	if currentLastLogIndex == 0 && args.PrevLogIndex != 0 {
		extraInfo.XLen = 0
		return false, false, extraInfo
	}

	// If the peers log length is >= leaders prev log index.
	if currentLogLen >= args.PrevLogIndex {
		currentPrevLogEntry := rf.logContents[args.PrevLogIndex-1]
		// If the prev entry in the peers log and leaders prev entry match.
		if currentPrevLogEntry.LogIndex == args.PrevLogIndex && currentPrevLogEntry.Term == args.PrevLogTerm {
			modified := rf.InsertEntries(currentLogLen, args)
			// Only truncate the logs if the incoming entries modified the servers log.
			// Otherwise and out of order request can wipe out legit entries from the servers log.
			if modified {
				rf.logContents = rf.logContents[:lastEntryFromLeader.LogIndex]
			}
			rf.Log("appended to follower[%d] log. Current log size: %d", rf.me, len(rf.logContents))
			return true, modified, extraInfo
		} else {
			// Prev entries dont match. We also remove the conflicting entry from the peers log.
			extraInfo.Xterm = currentPrevLogEntry.Term
			extraInfo.XIndex = rf.getFirstEntryWithTerm(currentPrevLogEntry.Term).LogIndex
			rf.Log("Prev entry's index and/or term doesnt match with the leader.")
			return false, false, extraInfo
		}
	}
	extraInfo.XLen = currentLogLen
	return false, false, extraInfo
}

func (rf *Raft) EntriesMatch(a, b LogEntry) bool {
	return a == b
}

func (rf *Raft) EntriesDontMatch(a, b LogEntry) bool {
	return a != b
}

func (rf *Raft) InsertEntries(currentLogLen int, args *AppendEntriesArgs) bool {
	logModified := false
	for _, incomingEntry := range args.Entries {
		if currentLogLen >= incomingEntry.LogIndex {
			if rf.EntriesDontMatch(incomingEntry, rf.logContents[incomingEntry.LogIndex-1]) {
				rf.logContents[incomingEntry.LogIndex-1] = incomingEntry
				logModified = true
			}
		} else {
			rf.logContents = append(rf.logContents, incomingEntry)
			logModified = true
		}
	}
	return logModified
}

func (rf *Raft) acknowledgeHeartBeat(args *AppendEntriesArgs) {
	if rf.IsFollower() {
		rf.blockingNotifyWhile(rf.heartBeatChan, "acking heart beat from leader", rf.IsFollower)
	}
	if args.Term > rf.currentTerm {
		rf.setTerm(args.Term)
	}

	if rf.IsElectionRunning() {
		rf.Log("asked to stop election and tranistioning to follower")
		rf.SafeTransitionWithMutex(rf.TransitionToFollower, fmt.Sprintf("from ack heartbeat [%s]from candidate with term: %d", rf.candidateID, rf.currentTerm))
	}
	if rf.IsLeader() {
		rf.SafeTransitionWithMutex(rf.TransitionToFollower, fmt.Sprintf("from ack heartbeat [%s]from leader with term: %d", rf.candidateID, rf.currentTerm))
		rf.Log("asked to transition from leader to follower")
	}

	if rf.getCurrentLeader() == nil || rf.getCurrentLeader() != rf.peers[args.LeaderID] {
		rf.setCurrentLeader(rf.peers[args.LeaderID])
	}
	rf.Log("acknowledged heartbeat from leader: %d leader commitIndex: %d, my log length: %d", args.LeaderID, args.LeaderCommit, rf.getLogLen())
}

func (rf *Raft) UpdateCommitIndex(leaderCommitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentLogLen := rf.logLength()
	rf.commitIndex = min(currentLogLen, leaderCommitIndex)
	rf.ApplyToStateMachine()
}

func (rf *Raft) updateCommitIndex(leaderCommitIndex int) {
	currentLogLen := rf.logLength()
	rf.commitIndex = min(currentLogLen, leaderCommitIndex)
	rf.ApplyToStateMachine()
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
