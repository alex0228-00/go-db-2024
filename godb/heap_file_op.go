package godb

import (
	"fmt"
)

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	hp, err := f.getFreePage(tid, WritePerm)
	if err != nil {
		return fmt.Errorf("insertTuple: error getting free page: %v", err)
	}

	if _, err = hp.insertTuple(t); err != nil {
		return fmt.Errorf("insertTuple: error inserting tuple into page: %v", err)
	}

	if hp.freelist.IsFull() {
		f.full.Store(hp.pageNo, struct{}{})
	}
	return nil
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	rid, ok := t.Rid.(*RecordId)
	assert(ok, "deleteTuple: expected RecordId, got %T", t.Rid)
	assert(
		rid.PageNo <= f.NumPages(),
		"deleteTuple: record ID page number %d is greater than number of pages %d", rid.PageNo, f.NumPages(),
	)

	hp, err := f.getPage(rid.PageNo, tid, WritePerm)
	if err != nil {
		return fmt.Errorf("deleteTuple: error getting page %d from heap file: %v", rid.PageNo, err)
	}

	if err := hp.deleteTuple(rid); err != nil {
		return fmt.Errorf("deleteTuple: error deleting tuple from page %d: %v", rid.PageNo, err)
	}
	return nil
}

func (f *HeapFile) getFreePage(tid TransactionID, perm RWPerm) (*heapPage, error) {
	for i := 1; ; i++ {
		if _, ok := f.full.Load(i); ok {
			continue
		}

		hp, err := f.getPage(i, tid, perm)
		if err != nil {
			return nil, fmt.Errorf("getFreePageFromCurrentPages: error getting page %d from heap file: %v", i, err)
		}

		if !hp.freelist.IsFull() {
			return hp, nil
		} else {
			f.full.Store(i, struct{}{})
			f.bufPool.lock.Unlock(tid, f.pageKey(i).(heapHash), perm)
		}
	}
}

func (f *HeapFile) getPage(pageNo int, tid TransactionID, perm RWPerm) (*heapPage, error) {
	p, err := f.bufPool.GetPage(f, pageNo, tid, perm)
	if err != nil {
		return nil, fmt.Errorf("getPage: error getting page %d from buffer pool: %v", pageNo, err)
	}
	hp, ok := p.(*heapPage)
	assert(ok, "getPage: expected heapPage, got %T", p)
	return hp, nil
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
// Make sure to set the returned tuple's TupleDescriptor to the TupleDescriptor of
// the HeapFile. This allows it to correctly capture the table qualifier.
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	curPage := 0
	var fn func() (*Tuple, error)

	return func() (*Tuple, error) {
		for {
			if fn != nil {
				t, err := fn()
				if err != nil {
					return nil, fmt.Errorf("Iterator: error iterating over heap file: %v", err)
				}
				if t != nil {
					return t, nil
				}
				fn = nil

				f.bufPool.lock.Unlock(tid, f.pageKey(curPage).(heapHash), ReadPerm)
				assert(err == nil, "Iterator: error unlocking page %d: %v", curPage, err)
			}

			curPage++
			if curPage > f.NumPages() {
				return nil, nil
			}

			p, err := f.bufPool.GetPage(f, curPage, tid, ReadPerm)
			if err != nil {
				return nil, fmt.Errorf("Iterator: error getting page %d from buffer pool: %v", curPage, err)
			}

			hp, ok := p.(*heapPage)
			assert(ok, "Iterator: expected heapPage, got %T", p)

			fn = hp.tupleIter()
		}

	}, nil
}
