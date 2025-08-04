package godb

import (
	"bytes"
	"fmt"

	"github.com/bits-and-blooms/bitset"
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

type heapPage struct {
	f       *HeapFile
	desc    *TupleDesc
	pageNo  int
	ntuples int // number of tuples on the page

	dirty    bool
	tuples   map[int]*Tuple
	freelist *FreeList
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	page := &heapPage{
		f:      f,
		desc:   desc,
		pageNo: pageNo,
		tuples: make(map[int]*Tuple),
		dirty:  false,
	}

	ntuples := page.calTupleSize(desc)
	if ntuples < 0 {
		return nil, fmt.Errorf("tuple size exceeds page size")
	}

	page.ntuples = ntuples
	page.freelist = NewFreeList(ntuples)
	return page, nil
}

func (h *heapPage) calTupleSize(desc *TupleDesc) int {
	size := desc.Size()
	ntuples := PageSize / size

	for ; ntuples > 0; ntuples-- {
		total := ntuples*size + 8*(divide(ntuples, 64)+1)
		if total <= PageSize {
			return ntuples
		}
	}

	return -1
}

func (h *heapPage) getNumSlots() int {
	return h.ntuples
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

	h.tuples[index.Slot] = nil

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

	w, err := h.freelist.WriteTo(buf)
	if err != nil {
		return nil, fmt.Errorf("error writing freelist: %v", err)
	}

	writed := int(w)
	for i := 0; i < h.ntuples; i++ {
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
	read, err := h.freelist.ReadFrom(buf)
	if err != nil {
		return fmt.Errorf("error initializing freelist from buffer: %v", err)
	}

	for i := 0; i < h.ntuples; i++ {
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
			if cur >= p.getNumSlots() {
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
	bitmap    *bitset.BitSet
	available []int
}

func NewFreeList(n int) *FreeList {
	free := &FreeList{
		bitmap:    bitset.New(uint(n)),
		available: make([]int, 0, n),
	}

	for i := 0; i < n; i++ {
		free.available = append(free.available, i)
	}
	return free
}

func (free *FreeList) ReadFrom(buf *bytes.Buffer) (int, error) {
	read, err := free.bitmap.ReadFrom(buf)
	if err != nil {
		return -1, fmt.Errorf("error reading bitmap from buffer: %v", err)
	}

	for i := 0; i < int(free.bitmap.Len()); i++ {
		if !free.bitmap.Test(uint(i)) {
			free.available = append(free.available, i)
		}
	}
	return int(read), nil
}

func (free *FreeList) Test(n int) bool {
	return free.bitmap.Test(uint(n))
}

func (free *FreeList) IsFull() bool {
	return len(free.available) == 0
}

func (free *FreeList) GetSlot() int {
	assert(!free.IsFull(), "FreeList is full, cannot get slot")

	slot := free.available[0]
	free.available = free.available[1:]

	free.bitmap.Set(uint(slot))
	return slot
}

func (free *FreeList) ReleaseSlot(slot int) error {
	assert(slot >= 0 && slot < int(free.bitmap.Len()), "Invalid slot number")

	if !free.bitmap.Test(uint(slot)) {
		return fmt.Errorf("slot %d is already free", slot)
	}

	free.bitmap.Clear(uint(slot))
	free.available = append(free.available, slot)
	return nil
}

func (free *FreeList) WriteTo(buf *bytes.Buffer) (int64, error) {
	return free.bitmap.WriteTo(buf)
}

func divide(a int, b int) int {
	ret := a / b
	if a%b != 0 {
		ret++
	}
	return ret
}
