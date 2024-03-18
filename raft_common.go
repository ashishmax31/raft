package raft

func (rf *Raft) LastLogIndex() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return len(rf.logContents)
}

func (rf *Raft) GetCommitIndex() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.commitIndex
}

func (rf *Raft) GetLogEntryAt(at int) LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.logContents[at]
}

func (rf *Raft) LastLogTerm() int32 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if len(rf.logContents) == 0 {
		return 0
	}
	return rf.logContents[len(rf.logContents)-1].Term
}

func (rf *Raft) lastLogTerm() int32 {
	if len(rf.logContents) == 0 {
		return 0
	}
	return rf.logContents[len(rf.logContents)-1].Term
}

func (rf *Raft) LogLength() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return len(rf.logContents)
}

func (rf *Raft) logLength() int {
	return len(rf.logContents)
}

func (rf *Raft) GetCurrentTerm() int32 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) getCurrentTerm() int32 {
	return rf.currentTerm
}

func (rf *Raft) Peers() []RaftPeer {
	return rf.peers
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) GetCandidateID() string {
	return rf.candidateID
}

func (rf *Raft) VoteForSelf() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = &VotedFor{
		Candidate: rf.candidateID,
		Term:      rf.currentTerm,
	}
	rf.persist()
}

func (rf *Raft) voteForSelf() {
	rf.votedFor = &VotedFor{
		Candidate: rf.candidateID,
		Term:      rf.currentTerm,
	}
	rf.persist()
}

func (rf *Raft) SetTerm(to int32) int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if to > rf.currentTerm {
		rf.currentTerm = to
		rf.votedFor = nil
		rf.persist()
	}
	return rf.currentTerm
}

func (rf *Raft) setTerm(to int32) int32 {
	if to > rf.currentTerm {
		rf.currentTerm = to
		rf.votedFor = nil
		rf.persist()
	}
	return rf.currentTerm
}

func (rf *Raft) IncrementTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.votedFor = nil
	rf.persist()
}

func (rf *Raft) incrementTerm() {
	rf.currentTerm += 1
	rf.votedFor = nil
	rf.persist()
}

func (rf *Raft) GetCurrentLeader() RaftPeer {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentLeader
}

func (rf *Raft) getCurrentLeader() RaftPeer {
	return rf.currentLeader
}

func (rf *Raft) SetCurrentLeader(leader RaftPeer) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentLeader = leader
}

func (rf *Raft) setCurrentLeader(leader RaftPeer) {
	rf.currentLeader = leader
}

func (rf *Raft) GetFirstEntryWithTerm(term int32) LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getFirstEntryWithTerm(term)
}

func (rf *Raft) getFirstEntryWithTerm(term int32) LogEntry {
	for _, entry := range rf.logContents {
		if entry.Term == term {
			return entry
		}
	}
	return LogEntry{
		LogIndex: -1,
	}
}

func (rf *Raft) GetLastEntryWithTerm(term int32) *LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getLastEntryWithTerm(term)
}

func (rf *Raft) getLastEntryWithTerm(term int32) *LogEntry {
	currLen := len(rf.logContents)
	for i := currLen - 1; i >= 0; i-- {
		curr := rf.logContents[i]
		if curr.Term == term {
			return &curr
		}
	}
	return nil
}

func (rf *Raft) getLogLen() int {
	return len(rf.logContents)
}

func (rf *Raft) GetLogLen() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return len(rf.logContents)
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

// The caller needs to hold the mutex when calling this method.
func (rf *Raft) SafeTransitionWithMutex(fn StateTransitionFn, info string) {
	rf.mu.Unlock()
	fn(info)
	rf.mu.Lock()
}
