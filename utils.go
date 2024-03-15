package raft

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

func (rf *Raft) blockingNotifyWithAck(channel chan struct{}, action string) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancelFn()
	select {
	case <-rf.killedChan:
		return
	case channel <- struct{}{}:
		select {
		case <-rf.killedChan:
			return
		case <-channel:
			return
		case <-ctx.Done():
			err := fmt.Sprintf("[id: %d][action: %s] failed to blocking notify ack", rf.me, action)
			panic(err)
		}
	case <-ctx.Done():
		err := fmt.Sprintf("[id: %d][action: %s] failed to blocking notify", rf.me, action)
		panic(err)
	}
}

func (rf *Raft) blockingNotify(channel chan struct{}, action string) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancelFn()
	select {
	case <-rf.killedChan:
		return
	case channel <- struct{}{}:
		return
	case <-ctx.Done():
		err := fmt.Sprintf("[id: %d][action: %s] failed to blocking notify", rf.me, action)
		panic(err)
	}
}

func (rf *Raft) blockingNotifyWhile(channel chan struct{}, action string, condFn func() bool) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancelFn()
	for condFn() {
		select {
		case <-rf.killedChan:
			return
		case channel <- struct{}{}:
			return
		case <-ctx.Done():
			err := fmt.Sprintf("[id: %d][action: %s] failed to blocking notify", rf.me, action)
			panic(err)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}

}

func (rf *Raft) sendAck(channel chan struct{}) {
	ctx, cancelFn := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancelFn()
	select {
	case <-rf.killedChan:
		return
	case channel <- struct{}{}:
		return
	case <-ctx.Done():
		panic("failed to sendack to channel")
	}
}

func createReplicatorID(from int, to int) int {
	numStr := fmt.Sprintf("%d%d", from, to)
	num, err := strconv.Atoi(numStr)
	if err != nil {
		panic(err)
	}
	return num
}
