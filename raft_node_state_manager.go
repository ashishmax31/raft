package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const timeoutDuration = time.Millisecond * 100

type StateTransitionFn func(info string)

type StateManager interface {
	IsFollower() bool
	IsLeader() bool
	IsElectionRunning() bool
	TransitionToCandidate(info string)
	TransitionToLeader(info string)
	TransitionToFollower(info string)
	LeaderChan() chan struct{}
	CandidateChan() chan struct{}
	FollowerChan() chan struct{}
	ReleaseElectionRunningFlag()
	ReleaseLeaderFlag()
	ReleaseFollowerFlag()
	SendTransitionedToFollowerAck()
	SendElectionRunningAck()
	SendTransitionedToLeaderAck()
}

type stateManager struct {
	ID                        int
	raftKilledChan            chan struct{}
	followerRoutineRunning    atomic.Bool
	leaderRoutineRunning      atomic.Bool
	electionRunning           atomic.Bool
	candidateChan             chan struct{}
	followerChan              chan struct{}
	leaderChan                chan struct{}
	leaderAckChannel          chan struct{}
	followerAckChannel        chan struct{}
	electionRunningAckChannel chan struct{}
	stopLeaderChan            chan struct{}
	stopElectionChan          chan struct{}
	stopFollowerChan          chan struct{}
	sync.RWMutex
}

func NewStateManager(
	ID int,
	stopLeaderChan,
	stopElectionChan,
	stopFollowerChan,
	raftKilledChan chan struct{}) StateManager {
	s := &stateManager{
		raftKilledChan:            raftKilledChan,
		candidateChan:             make(chan struct{}),
		leaderChan:                make(chan struct{}),
		followerChan:              make(chan struct{}),
		leaderAckChannel:          make(chan struct{}),
		followerAckChannel:        make(chan struct{}),
		electionRunningAckChannel: make(chan struct{}),
		stopLeaderChan:            stopLeaderChan,
		stopElectionChan:          stopElectionChan,
		stopFollowerChan:          stopFollowerChan,
	}
	return s
}

func (s *stateManager) IsFollower() bool {
	return s.followerRoutineRunning.Load()
}

func (s *stateManager) IsLeader() bool {
	return s.leaderRoutineRunning.Load()
}

func (s *stateManager) IsElectionRunning() bool {
	return s.electionRunning.Load()
}

func (s *stateManager) isFollower() bool {
	return s.followerRoutineRunning.Load()
}

func (s *stateManager) isLeader() bool {
	return s.leaderRoutineRunning.Load()
}

func (s *stateManager) isElectionRunning() bool {
	return s.electionRunning.Load()
}

func (s *stateManager) raiseLeaderFlag() {
	s.leaderRoutineRunning.Store(true)
}

func (s *stateManager) raiseElectionRunningFlag() {
	s.electionRunning.Store(true)
}

func (s *stateManager) raiseFollowerFlag() {
	s.followerRoutineRunning.Store(true)
}

func (s *stateManager) ReleaseLeaderFlag() {
	s.leaderRoutineRunning.Store(false)
}

func (s *stateManager) ReleaseElectionRunningFlag() {
	s.electionRunning.Store(false)
}

func (s *stateManager) ReleaseFollowerFlag() {

	s.followerRoutineRunning.Store(false)
}

func (s *stateManager) TransitionToCandidate(info string) {
	s.Lock()
	defer s.Unlock()
	if s.isElectionRunning() {
		return
	}
	if s.isLeader() {
		s.blockingNotifyWithAckWhile(s.stopLeaderChan, "stop leader, becoming candidate", s.isLeader)
	}
	if s.isFollower() {
		s.blockingNotifyWithAckWhile(s.stopFollowerChan, "stop follower, becoming candidate", s.isFollower)
	}
	ctx1, cancelFn1 := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancelFn1()
	select {
	case <-s.raftKilledChan:
		return
	case s.candidateChan <- struct{}{}:
		s.raiseElectionRunningFlag()
		break
	case <-ctx1.Done():
		panic("failed to transition to candidate")
	}
	ctx2, cancelFn2 := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancelFn2()
	select {
	case <-s.raftKilledChan:
		return
	case <-s.electionRunningAckChannel:
		break
	case <-ctx2.Done():
		panic("failed to receive ack from candidate")
	}
}

func (s *stateManager) TransitionToLeader(info string) {
	s.Lock()
	defer s.Unlock()
	if s.isLeader() {
		return
	}

	if s.isFollower() {
		s.blockingNotifyWithAckWhile(s.stopFollowerChan, fmt.Sprintf("[term: %d]stop follower, becoming leader", info), s.isFollower)
	}
	if s.isElectionRunning() {
		s.blockingNotifyWhile(s.stopElectionChan, fmt.Sprintf("[term: %d]stop election, becoming leader", info), s.isElectionRunning)
	}
	ctx1, cancelFn1 := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancelFn1()
	select {
	case <-s.raftKilledChan:
		return
	case s.leaderChan <- struct{}{}:
		s.raiseLeaderFlag()
		break
	case <-ctx1.Done():
		panic("failed to transition to leader")
	}
	ctx2, cancelFn2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelFn2()
	select {
	case <-s.raftKilledChan:
		return
	case <-s.leaderAckChannel:
		break
	case <-ctx2.Done():
		panic(fmt.Sprintf("[term: %d]failed to receive ack from leader", info))
	}
}

func (s *stateManager) TransitionToFollower(info string) {
	s.Lock()
	defer s.Unlock()
	if s.isFollower() {
		return
	}
	if s.isLeader() {
		s.blockingNotifyWithAckWhile(s.stopLeaderChan, info, s.isLeader)
	}
	if s.isElectionRunning() {
		s.blockingNotifyWhile(s.stopElectionChan, info, s.isElectionRunning)
	}

	ctx1, cancelFn1 := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancelFn1()
	select {
	case <-s.raftKilledChan:
		return
	case s.followerChan <- struct{}{}:
		s.raiseFollowerFlag()
		break
	case <-ctx1.Done():
		panic(fmt.Sprintf("failed to transition to follower. info: %v", info))
	}

	ctx2, cancelFn2 := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancelFn2()
	select {
	case <-s.raftKilledChan:
		return
	case <-s.followerAckChannel:
		break
	case <-ctx2.Done():
		panic(fmt.Sprintf("failed to receive ack from follower. info: %v", info))
	}
}

func (s *stateManager) LeaderChan() chan struct{} {
	return s.leaderChan
}
func (s *stateManager) CandidateChan() chan struct{} {
	return s.candidateChan
}
func (s *stateManager) FollowerChan() chan struct{} {
	return s.followerChan
}

func (s *stateManager) SendTransitionedToFollowerAck() {
	select {
	case <-s.raftKilledChan:
		return
	case s.followerAckChannel <- struct{}{}:
		return
	}
}

func (s *stateManager) SendElectionRunningAck() {
	select {
	case <-s.raftKilledChan:
		return
	case s.electionRunningAckChannel <- struct{}{}:
		return
	}
}

func (s *stateManager) SendTransitionedToLeaderAck() {
	select {
	case <-s.raftKilledChan:
		return
	case s.leaderAckChannel <- struct{}{}:
		return
	}
}

func (s *stateManager) blockingNotifyWithAckWhile(channel chan struct{}, action string, condFn func() bool) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancelFn()
	for condFn() {
		select {
		case <-s.raftKilledChan:
			return
		case channel <- struct{}{}:
			select {
			case <-s.raftKilledChan:
				return
			case <-channel:
				return
			case <-ctx.Done():
				err := fmt.Sprintf("[id: %d][action: %s] failed to blocking notify ack", s.ID, action)
				panic(err)
			}
		case <-ctx.Done():
			err := fmt.Sprintf("[id: %d][action: %s] failed to blocking notify", s.ID, action)
			panic(err)
		default:
		}
		time.Sleep(time.Millisecond * 5)
	}
}

func (s *stateManager) blockingNotifyWhile(channel chan struct{}, action string, condFn func() bool) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancelFn()
	for condFn() {
		select {
		case <-s.raftKilledChan:
			return
		case channel <- struct{}{}:
			return
		case <-ctx.Done():
			err := fmt.Sprintf("[id: %d][action: %s] failed to blocking notify", s.ID, action)
			panic(err)
		default:
		}
		time.Sleep(time.Millisecond * 5)
	}

}
