package raft

import (
	"context"
	"sync"
)

type GoRoutineManager interface {
	AddRoutine(id ID) (context.Context, chan struct{})
	StopAll()
	Exists(id ID) bool
	Stop(condFn CondFn)
}

type ID int

type goRoutineCntrl struct {
	cancelFn      func()
	cancelableCtx context.Context
	exitedChan    chan struct{}
}

type CondFn func(id ID) bool

type goRoutineManager struct {
	sync.RWMutex
	goRountineCntrls map[ID]goRoutineCntrl
}

func NewGoRoutineManager() GoRoutineManager {
	return &goRoutineManager{
		goRountineCntrls: map[ID]goRoutineCntrl{},
	}
}

func (mgr *goRoutineManager) AddRoutine(id ID) (context.Context, chan struct{}) {
	mgr.Lock()
	defer mgr.Unlock()
	ctx, cancelFn := context.WithCancel(context.Background())
	exitChan := make(chan struct{})
	mgr.goRountineCntrls[id] = goRoutineCntrl{
		cancelableCtx: ctx,
		cancelFn:      cancelFn,
		exitedChan:    exitChan,
	}
	go func() {
		<-exitChan
		mgr.Lock()
		defer mgr.Unlock()
		delete(mgr.goRountineCntrls, id)
	}()
	return ctx, exitChan
}

func (mgr *goRoutineManager) StopAll() {
	go func() {
		mgr.Lock()
		defer mgr.Unlock()
		mgr.stop(mgr.goRountineCntrls)
	}()
}

func (mgr *goRoutineManager) Exists(id ID) bool {
	mgr.RLock()
	defer mgr.RUnlock()
	_, ok := mgr.goRountineCntrls[id]
	return ok
}

func (mgr *goRoutineManager) Stop(condFn CondFn) {
	go func() {
		mgr.Lock()
		defer mgr.Unlock()
		filteredGoRoutineCntrls := make(map[ID]goRoutineCntrl, 0)
		for id, ctrl := range mgr.goRountineCntrls {
			if condFn(id) {
				filteredGoRoutineCntrls[id] = ctrl
			}
		}
		mgr.stop(filteredGoRoutineCntrls)
	}()
}

func (mgr *goRoutineManager) stop(cntrls map[ID]goRoutineCntrl) {
	for _, cntrl := range cntrls {
		cntrl.cancelFn()
	}
}
