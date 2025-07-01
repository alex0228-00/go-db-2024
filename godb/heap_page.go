package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

const MaxSlotPerPage = 64

type heapPage struct {
	f      *HeapFile
	desc   *TupleDesc
	pageNo int

	dirty    bool
	tuples   map[int]*Tuple
	freelist *FreeList
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	page := &heapPage{
		f:        f,
		desc:     desc,
		pageNo:   pageNo,
		tuples:   make(map[int]*Tuple),
		dirty:    false,
		freelist: NewFreeList(),
	}
	return page, nil
}

func (h *heapPage) getNumSlots() int {
	return min((PageSize-h.freelist.Size())/h.desc.Size(), MaxSlotPerPage)
}

func (h *heapPage) getFreeSlot() int {
	if h.freelist.IsFull() {
		return -1 // no free slots available
	}
	return h.freelist.GetSlot()
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	if h.freelist.IsFull() {
		return nil, fmt.Errorf("no free slots available")
	}

	slot := h.getFreeSlot()

	h.tuples[slot] = t
	h.dirty = true

	t.Rid = &RecordId{PageNo: h.pageNo, Slot: slot}
	return t.Rid, nil
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	index := rid.(*RecordId)
	assert(index.PageNo == h.pageNo, "record ID does not match page number")

	if err := h.freelist.ReleaseSlot(index.Slot); err != nil {
		return fmt.Errorf("error releasing slot: %v", err)
	}
	h.dirty = true
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	return p.f
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(make([]byte, 0, PageSize))

	if err := h.freelist.WriteTo(buf); err != nil {
		return nil, fmt.Errorf("error writing freelist: %v", err)
	}

	writed := h.freelist.Size() // size of the freelist header
	for i := 0; i < MaxSlotPerPage; i++ {
		if h.freelist.Test(i) {
			t, ok := h.tuples[i]
			assert(ok, "tuple not found for slot %d", i)

			if err := t.writeTo(buf); err != nil {
				return nil, fmt.Errorf("error writing tuple to buffer: %v", err)
			}
			writed += t.Desc.Size()
		}
	}

	left := PageSize - writed
	if left > 0 {
		if n, err := buf.Write(make([]byte, left)); err != nil {
			return nil, fmt.Errorf("error writing padding to buffer: %v", err)
		} else if n != left {
			return nil, fmt.Errorf("expected to write %d bytes, but wrote %d", left, n)
		}
	}

	return buf, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	freelist, err := NewFreeListFromBuf(buf)
	if err != nil {
		return fmt.Errorf("error initializing freelist from buffer: %v", err)
	}
	h.freelist = freelist

	read := h.freelist.Size()
	for i := 0; i < MaxSlotPerPage; i++ {
		if h.freelist.Test(i) {
			t, err := readTupleFrom(buf, h.desc)
			if err != nil {
				return fmt.Errorf("error reading tuple from buffer: %v", err)
			}
			h.tuples[i] = t
			read += h.desc.Size()
		}
	}

	left := PageSize - read
	if left > 0 {
		buf.Truncate(left)
	}

	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	cur := -1
	return func() (*Tuple, error) {
		for {
			cur++
			if cur >= MaxSlotPerPage {
				return nil, nil
			}
			if p.freelist.Test(cur) {
				t, ok := p.tuples[cur]
				assert(ok, "tuple not found for slot %d", cur)
				return t, nil
			}
		}
	}
}

type FreeList struct {
	bitmap uint64
	free   []int
}

func NewFreeList() *FreeList {
	free := &FreeList{
		free: make([]int, 0, MaxSlotPerPage), // preallocate space for 64 slots
	}

	for i := 0; i < MaxSlotPerPage; i++ {
		free.free = append(free.free, i)
	}
	return free
}

func NewFreeListFromBuf(buf *bytes.Buffer) (*FreeList, error) {
	free := &FreeList{}

	if err := binary.Read(buf, binary.LittleEndian, &free.bitmap); err != nil {
		return nil, fmt.Errorf("error reading bitmap: %v", err)
	}

	for i := 0; i < MaxSlotPerPage; i++ {
		if (free.bitmap & (1 << i)) == 0 {
			free.free = append(free.free, i)
		}
	}
	return free, nil
}

func (free *FreeList) Test(n int) bool {
	return (free.bitmap & (1 << n)) != 0
}

func (free *FreeList) IsFull() bool {
	return free.bitmap == 0xFFFFFFFFFFFFFFFF
}

func (free *FreeList) GetSlot() int {
	assert(!free.IsFull(), "FreeList is full, cannot get slot")

	slot := free.free[0]
	free.free = free.free[1:]

	free.bitmap |= (1 << slot)
	return slot
}

func (free *FreeList) ReleaseSlot(slot int) error {
	assert(slot >= 0 && slot < MaxSlotPerPage, "Invalid slot number")

	if (free.bitmap & (1 << slot)) == 0 {
		return fmt.Errorf("slot %d is already free", slot)
	}

	free.bitmap &^= (1 << slot)         // Clear the bit for the slot
	free.free = append(free.free, slot) // Add the slot back to the free list
	return nil
}

func (free *FreeList) WriteTo(buf *bytes.Buffer) error {
	if err := binary.Write(buf, binary.LittleEndian, free.bitmap); err != nil {
		return fmt.Errorf("error writing bitmap: %v", err)
	}
	return nil
}

func (free *FreeList) Size() int {
	return 8 // Size of uint64 in bytes
}
