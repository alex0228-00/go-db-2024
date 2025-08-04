package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"fmt"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota

	AllPerm RWPerm = iota
)

type BufferPool struct {
	// TODO: some code goes here
	cap   int
	pages map[any]Page

	lock *DbLock
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		cap:   numPages,
		pages: make(map[any]Page, numPages),
		lock:  NewDbLock(),
	}, nil
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	for _, page := range bp.pages {
		if page.isDirty() {
			err := page.getFile().flushPage(page)

			assert(err == nil, "FlushAllPages: error flushing page: %v", err)
			page.setDirty(0, false)
		}
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	pageKeys := bp.lock.GetAllLockedPages(tid)

	for _, pageKey := range pageKeys {
		page, ok := bp.pages[pageKey]

		if ok && page.isDirty() {
			delete(bp.pages, pageKey)
		}
	}
	bp.lock.UnlockAll(tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	pageKeys := bp.lock.GetAllLockedPages(tid)

	for _, pageKey := range pageKeys {
		page, ok := bp.pages[pageKey]
		assert(ok, "CommitTransaction: page not found in buffer pool for key %v", pageKey)

		if page.isDirty() {
			err := page.getFile().flushPage(page)

			assert(err == nil, "CommitTransaction: error flushing page: %v", err)
			page.setDirty(0, false)
		}
	}

	bp.lock.UnlockAll(tid)
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	if err := bp.lock.Lock(tid, file.pageKey(pageNo).(heapHash), perm); err != nil {
		return nil, fmt.Errorf("GetPage: error locking page %d in file: %w", pageNo, err)
	}

	ret, ok := bp.pages[file.pageKey(pageNo)]
	if ok {
		return ret, nil
	}

	if err := bp.evictPageIfNeed(); err != nil {
		return nil, fmt.Errorf("GetPage: buffer pool is full, cannot evict any page: %w", err)
	}

	read, err := file.readPage(pageNo)
	if err != nil {
		return nil, fmt.Errorf("GetPage: error reading page %d from file: %w", pageNo, err)
	}

	bp.pages[file.pageKey(pageNo)] = read
	return read, nil
}

func (bp *BufferPool) evictPageIfNeed() error {
	if len(bp.pages) < bp.cap {
		return nil
	}

	tid := NewTID()
	evict := func(key heapHash, page Page) bool {
		if err := bp.lock.LockNoWait(tid, key, WritePerm); err == nil {
			defer bp.lock.Unlock(tid, key, WritePerm)

			if !page.isDirty() {
				delete(bp.pages, key)
				return true
			}
		}
		return false
	}
	for k, v := range bp.pages {
		if evict(k.(heapHash), v) {
			return nil
		}
	}
	return fmt.Errorf("GetPage: buffer pool is full of dirty pages, cannot evict any page")
}
