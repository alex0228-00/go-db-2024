package godb

import (
	"fmt"
	"slices"
	"sync"
)

var (
	ErrorDeadlock     = fmt.Errorf("deadlock detected")
	ErrorLockOccupied = fmt.Errorf("lock occupied")
)

type LockReq struct {
	Tid     TransactionID
	pageKey heapHash
	Perm    RWPerm
	Ch      chan error
}

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

func (wl *WaitList) Remove(tid TransactionID) {
	reqs, ok := wl.tidWaitList[tid]
	if !ok {
		return
	}

	delete(wl.tidWaitList, tid)
	for _, req := range reqs {
		wl.pageWaitList[req.pageKey] = slices.DeleteFunc(
			wl.pageWaitList[req.pageKey],
			func(r *LockReq) bool {
				return r.Tid == tid && r.pageKey == req.pageKey
			},
		)
	}
}

type dbLock struct {
	perm    RWPerm
	pageKey heapHash

	// only for shared locks
	tids map[TransactionID]int

	// only for exclusive locks
	tid   TransactionID
	read  int
	write int
}

func (l *dbLock) isShared() bool {
	return l.perm == ReadPerm
}

func (l *dbLock) holdBy(tid TransactionID) bool {
	if l.isShared() {
		return l.tids[tid] > 0
	}
	return l.tid == tid
}

func (l *dbLock) lock(tid TransactionID, perm RWPerm) error {
	if l.isShared() {
		l.tids[tid]++
	} else {
		l.tid = tid
		if perm == ReadPerm {
			l.read++
		} else {
			l.write++
		}
	}
	return nil
}

func (l *dbLock) holders() []TransactionID {
	if l.isShared() {
		holders := make([]TransactionID, 0, len(l.tids))
		for tid := range l.tids {
			holders = append(holders, tid)
		}
		return holders
	}
	return []TransactionID{l.tid}
}

func (l *dbLock) canUpgrade(tid TransactionID) bool {
	return l.isShared() && len(l.tids) <= 1 && l.tids[tid] > 0
}

func (l *dbLock) upgrade(tid TransactionID) error {
	assert(l.isShared(), "Cannot upgrade a non-shared lock")

	l.perm = WritePerm
	l.tid = tid
	l.read = l.tids[tid]
	l.write = 1

	l.tids = nil
	return nil
}

func (l *dbLock) downgrade() {
	assert(l.perm == WritePerm, "Cannot downgrade a non-exclusive lock")
	assert(l.write == 0, "Cannot downgrade a lock that is still held")

	l.perm = ReadPerm
	l.tid = 0

	if l.tids == nil {
		l.tids = make(map[TransactionID]int)
	}
	for tid := range l.tids {
		l.tids[tid] = l.read
	}
	l.read = 0
}

func (l *dbLock) unlock(tid TransactionID, perm RWPerm) {
	if l.isShared() {
		if perm == AllPerm || l.tids[tid] == 1 {
			delete(l.tids, tid)
		} else {
			l.tids[tid]--
		}
	} else {
		if perm == AllPerm {
			l.write = 0
			l.read = 0
		} else if perm == ReadPerm {
			l.read--
		} else {
			l.write--
			if l.write == 0 {
				l.downgrade()
			}
		}
	}
}

func (l *dbLock) shouldRelease() bool {
	return (l.isShared() && len(l.tids) == 0) || (!l.isShared() && l.write == 0)
}

type DbLock struct {
	mux sync.Mutex

	locks map[heapHash]*dbLock
	wl    *WaitList
}

func NewDbLock() *DbLock {
	return &DbLock{
		locks: make(map[heapHash]*dbLock),
		wl:    NewWaitList(),
	}
}

func (dl *DbLock) Lock(tid TransactionID, pageKey heapHash, perm RWPerm) error {
	dl.mux.Lock()

	if err := dl.tryLock(tid, pageKey, perm); err == nil {
		dl.mux.Unlock()
		return nil
	}

	if dl.deadLockCheck(tid, []*dbLock{dl.locks[pageKey]}) {
		dl.mux.Unlock()
		return ErrorDeadlock
	}

	req := &LockReq{
		Tid:     tid,
		pageKey: pageKey,
		Perm:    perm,
		Ch:      make(chan error, 1),
	}
	dl.wl.Enqueue(req)
	dl.mux.Unlock()
	return <-req.Ch
}

func (dl *DbLock) Unlock(tid TransactionID, pageKey heapHash, perm RWPerm) {
	dl.mux.Lock()
	defer dl.mux.Unlock()

	dl.unlock(tid, pageKey, perm)
}

func (dl *DbLock) UnlockAll(tid TransactionID) {
	dl.mux.Lock()
	defer dl.mux.Unlock()

	dl.wl.Remove(tid)

	for pageKey, lock := range dl.locks {
		if lock.holdBy(tid) {
			dl.unlock(tid, pageKey, AllPerm)
		}
	}
}

func (dl *DbLock) LockNoWait(tid TransactionID, pageKey heapHash, perm RWPerm) error {
	dl.mux.Lock()
	defer dl.mux.Unlock()

	return dl.tryLock(tid, pageKey, perm)
}

func (dl *DbLock) unlock(tid TransactionID, pageKey heapHash, perm RWPerm) {
	lock, ok := dl.locks[pageKey]
	assert(ok, fmt.Sprintf("Unlock: no lock for page %v", pageKey))
	assert(lock.holdBy(tid), fmt.Sprintf("Unlock: transaction %v does not hold lock for page %v", tid, pageKey))

	lock.unlock(tid, perm)

	if lock.shouldRelease() {
		delete(dl.locks, pageKey)
		dl.notify(pageKey)
	}
}

func (dl *DbLock) notify(pageKey heapHash) {
	next := dl.wl.Dequeue(pageKey, AllPerm)
	for ; next != nil; next = dl.wl.Dequeue(pageKey, AllPerm) {
		if err := dl.tryLock(next.Tid, next.pageKey, next.Perm); err != nil {
			return
		}
		next.Ch <- nil
	}
}

func (dl *DbLock) deadLockCheck(origin TransactionID, locks []*dbLock) bool {
	holders := make(map[TransactionID]struct{})

	for _, lock := range locks {
		for _, tid := range lock.holders() {
			holders[tid] = struct{}{}
		}
	}
	locks = locks[:0]

	if len(holders) == 0 {
		return false
	}
	if _, ok := holders[origin]; ok {
		return true
	}
	for tid := range holders {
		reqs, ok := dl.wl.tidWaitList[tid]
		if !ok {
			continue
		}

		for _, req := range reqs {
			lock, ok := dl.locks[req.pageKey]
			assert(ok, fmt.Sprintf("deadlock check for non-locked page %v", req.pageKey))

			locks = append(locks, lock)
		}
	}

	return dl.deadLockCheck(origin, locks)
}

func (dl *DbLock) tryLock(tid TransactionID, pageKey heapHash, perm RWPerm) error {
	lock, ok := dl.locks[pageKey]
	if !ok {
		return dl.createNewLock(tid, pageKey, perm)
	}

	if lock.holdBy(tid) {
		if lock.perm >= perm {
			return lock.lock(tid, perm)
		} else if lock.canUpgrade(tid) {
			return lock.upgrade(tid)
		}
	}

	if lock.isShared() &&
		perm == ReadPerm &&
		dl.wl.NoWaitReq(pageKey) {
		return lock.lock(tid, perm)

	}
	return ErrorLockOccupied
}

func (dl *DbLock) createNewLock(tid TransactionID, pageKey heapHash, perm RWPerm) error {
	lock := &dbLock{
		perm:    perm,
		pageKey: pageKey,
	}

	if perm == WritePerm {
		lock.tid = tid
		lock.write = 1
	} else {
		lock.tids = make(map[TransactionID]int)
		lock.tids[tid] = 1
	}

	dl.locks[pageKey] = lock
	return nil
}

func (dl *DbLock) GetAllLockedPages(tid TransactionID) []heapHash {
	dl.mux.Lock()
	defer dl.mux.Unlock()

	var pageKeys []heapHash
	for pageKey, lock := range dl.locks {
		if lock.holdBy(tid) {
			pageKeys = append(pageKeys, pageKey)
		}
	}
	return pageKeys
}
