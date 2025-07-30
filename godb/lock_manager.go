package godb

import (
	"fmt"
	"slices"
)

var ErrorDeadlock = fmt.Errorf("deadlock detected")

type WaitList struct {
	tidWaitList  map[TransactionID][]*LockReq
	pageWaitList map[heapHash][]*LockReq
}

func NewWaitList() *WaitList {
	return &WaitList{
		tidWaitList:  make(map[TransactionID][]*LockReq),
		pageWaitList: make(map[heapHash][]*LockReq),
	}
}

func (wl *WaitList) Enqueue(req *LockReq) {
	if wl.tidWaitList[req.Tid] == nil {
		wl.tidWaitList[req.Tid] = []*LockReq{}
	}
	wl.tidWaitList[req.Tid] = append(wl.tidWaitList[req.Tid], req)

	if wl.pageWaitList[req.pageKey] == nil {
		wl.pageWaitList[req.pageKey] = []*LockReq{}
	}
	wl.pageWaitList[req.pageKey] = append(wl.pageWaitList[req.pageKey], req)
}

func (wl *WaitList) Dequeue(pageKey heapHash, expected RWPerm) *LockReq {
	if waitList, ok := wl.pageWaitList[pageKey]; ok && len(waitList) > 0 {
		nextReq := waitList[0]

		if expected != AllPerm && nextReq.Perm != expected {
			return nil
		}

		wl.pageWaitList[pageKey] = waitList[1:]
		wl.tidWaitList[nextReq.Tid] = slices.DeleteFunc(
			wl.tidWaitList[nextReq.Tid],
			func(r *LockReq) bool {
				return r.pageKey == pageKey && r.Tid == nextReq.Tid
			},
		)
		return nextReq
	}
	return nil
}

func (wl *WaitList) NoWaitReq(pageKey heapHash) bool {
	return wl.pageWaitList[pageKey] == nil || len(wl.pageWaitList[pageKey]) == 0
}

type LockManager struct {
	ReqCh    chan *LockReq
	pages    map[heapHash]*Lock
	waitlist *WaitList
}

type Lock struct {
	Shared  bool
	Tid     []TransactionID
	pageKey heapHash
}

type LockReq struct {
	Tid     TransactionID
	pageKey heapHash
	Perm    RWPerm
	Lock    bool
	Ch      chan error
}

func NewLockManager() *LockManager {
	lm := &LockManager{
		ReqCh:    make(chan *LockReq),
		pages:    make(map[heapHash]*Lock),
		waitlist: NewWaitList(),
	}
	go lm.daemon()
	return lm
}

func (lm *LockManager) daemon() {
	for req := range lm.ReqCh {
		if req.Lock {
			lm.handleLockReq(req)
		} else {
			lm.handleUnlockReq(req)
		}
	}
}

func (lm *LockManager) Lock(tid TransactionID, pageKey heapHash, perm RWPerm) error {
	req := &LockReq{
		Tid:     tid,
		pageKey: pageKey,
		Perm:    perm,
		Ch:      make(chan error, 1),
		Lock:    true,
	}
	lm.ReqCh <- req
	return <-req.Ch
}

func (lm *LockManager) Unlock(tid TransactionID, pageKey heapHash, perm RWPerm) error {
	req := &LockReq{
		Tid:     tid,
		pageKey: pageKey,
		Perm:    perm,
		Ch:      make(chan error, 1),
		Lock:    false,
	}
	lm.ReqCh <- req
	return <-req.Ch
}

func (lm *LockManager) handleLockReq(req *LockReq) {
	lock, ok := lm.pages[req.pageKey]
	if !ok {
		lm.doLock(req)
		return
	}

	if req.Perm == ReadPerm && lock.Shared && lm.waitlist.NoWaitReq(req.pageKey) {
		lm.doLock(req)
		return
	}

	if lm.deadLockCheck(req, lock) {
		req.Ch <- ErrorDeadlock
	} else {
		lm.waitlist.Enqueue(req)
	}
}

func (lm *LockManager) doLock(req *LockReq) {
	req.Ch <- nil

	lock, ok := lm.pages[req.pageKey]
	if !ok {
		lock = &Lock{
			Shared:  req.Perm == ReadPerm,
			pageKey: req.pageKey,
		}
	}

	lock.Tid = append(lock.Tid, req.Tid)
	lm.pages[req.pageKey] = lock
}

func (lm *LockManager) deadLockCheck(req *LockReq, current *Lock) bool {
	next := make(map[TransactionID]struct{})
	for _, tid := range current.Tid {
		next[tid] = struct{}{}
	}

	return lm._deadLockCheck(next, req.Tid)
}

func (lm *LockManager) _deadLockCheck(holders map[TransactionID]struct{}, origin TransactionID) bool {
	if len(holders) == 0 {
		return false
	}
	if _, ok := holders[origin]; ok {
		return true
	}

	next := make(map[TransactionID]struct{})
	for tid := range holders {
		reqs, ok := lm.waitlist.tidWaitList[tid]
		if !ok {
			continue
		}

		for _, req := range reqs {
			lock, ok := lm.pages[req.pageKey]
			assert(ok, fmt.Sprintf("deadlock check for non-locked page %v", req.pageKey))

			for _, tid := range lock.Tid {
				next[tid] = struct{}{}
			}
		}
	}

	return lm._deadLockCheck(next, origin)

}

func (lm *LockManager) handleUnlockReq(req *LockReq) {
	lock, ok := lm.pages[req.pageKey]

	assert(ok, fmt.Sprintf("unlocking non-locked page %v", req.pageKey))
	assert(lock.Shared == (req.Perm == ReadPerm), "unlocking with wrong permission")

	if !lock.Shared || len(lock.Tid) == 1 {
		delete(lm.pages, req.pageKey)
	} else {
		lock.Tid = slices.DeleteFunc(lock.Tid, func(tid TransactionID) bool {
			return tid == req.Tid
		})
	}

	req.Ch <- nil

	next := lm.waitlist.Dequeue(req.pageKey, AllPerm)
	if next != nil {
		lm.doLock(next)

		if next.Perm == ReadPerm {
			for {
				next = lm.waitlist.Dequeue(req.pageKey, ReadPerm)
				if next == nil {
					break
				}
				lm.doLock(next)
			}
		}
	}
}
