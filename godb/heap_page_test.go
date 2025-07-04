package godb

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestHeapPageInsert(t *testing.T) {
	td, t1, t2, hf, _, _ := makeTestVars(t)
	pg, err := newHeapPage(&td, 0, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	var expectedSlots = (PageSize - 8) / (StringLength + int(unsafe.Sizeof(int64(0))))
	if pg.getNumSlots() != expectedSlots {
		t.Logf("[Warning] Incorrect number of slots, expected %d, got %d", expectedSlots, pg.getNumSlots())
	}

	_, err = pg.insertTuple(&t1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, err = pg.insertTuple(&t2)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter := pg.tupleIter()
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, err := iter()
		if err != nil {
			t.Fatalf(err.Error())
		}
		if tup == nil {
			break
		}

		cnt += 1
	}
	if cnt != 2 {
		t.Errorf("Expected 2 tuples in interator, got %d", cnt)
	}
}

func TestHeapPageDelete(t *testing.T) {
	td, t1, t2, hf, _, _ := makeTestVars(t)
	pg, err := newHeapPage(&td, 0, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}

	pg.insertTuple(&t1)
	slotNo, _ := pg.insertTuple(&t2)
	pg.deleteTuple(slotNo)

	iter := pg.tupleIter()
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	cnt := 0
	for {

		tup, _ := iter()
		if tup == nil {
			break
		}

		cnt += 1
	}
	if cnt != 1 {
		t.Errorf("Expected 1 tuple in interator, got %d", cnt)
	}
}

// Unit test for insertTuple
func TestHeapPageInsertTuple(t *testing.T) {
	td, t1, _, hf, _, _ := makeTestVars(t)
	page, err := newHeapPage(&td, 0, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	free := page.getNumSlots()

	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)

		iter := page.tupleIter()
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		cnt, found := 0, false
		for {

			tup, _ := iter()
			found = found || addition.equals(tup)
			if tup == nil {
				break
			}

			cnt += 1
		}
		if cnt != i+1 {
			t.Errorf("Expected %d tuple in interator, got %d", i+1, cnt)
		}
		if !found {
			t.Errorf("Expected inserted tuple to be FOUND, got NOT FOUND")
		}
	}

	_, err = page.insertTuple(&t1)

	if err == nil {
		t.Errorf("Expected error due to full page")
	}
}

// Unit test for deleteTuple
func TestHeapPageDeleteTuple(t *testing.T) {
	td, _, _, hf, _, _ := makeTestVars(t)
	page, err := newHeapPage(&td, 0, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	free := page.getNumSlots()

	list := make([]recordID, free)
	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		list[i], _ = page.insertTuple(&addition)
	}
	if len(list) == 0 {
		t.Fatalf("Rid list is empty.")
	}
	for i, rnd := free-1, 0xdefaced; i > 0; i, rnd = i-1, (rnd*0x7deface1+12354)%0x7deface9 {
		// Generate a random index j such that 0 <= j <= i.
		j := rnd % (i + 1)

		// Swap arr[i] and arr[j].
		list[i], list[j] = list[j], list[i]
	}

	for _, rid := range list {
		err := page.deleteTuple(rid)
		if err != nil {
			t.Errorf("Found error %s", err.Error())
		}
	}

	err = page.deleteTuple(list[0])
	if err == nil {
		t.Errorf("page should be empty; expected error")
	}
}

// Unit test for isDirty, setDirty
func TestHeapPageDirty(t *testing.T) {
	td, _, _, hf, _, _ := makeTestVars(t)
	page, err := newHeapPage(&td, 0, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}

	page.setDirty(0, true)
	if !page.isDirty() {
		t.Errorf("page should be dirty")
	}
	page.setDirty(0, true)
	if !page.isDirty() {
		t.Errorf("page should be dirty")
	}
	page.setDirty(-1, false)
	if page.isDirty() {
		t.Errorf("page should be not dirty")
	}
}

// Unit test for toBuffer and initFromBuffer
func TestHeapPageSerialization(t *testing.T) {
	td, _, _, hf, _, _ := makeTestVars(t)
	page, err := newHeapPage(&td, 0, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	free := page.getNumSlots()

	for i := 0; i < free-1; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)
	}

	buf, _ := page.toBuffer()
	page2, err := newHeapPage(&td, 0, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = page2.initFromBuffer(buf)
	if err != nil {
		t.Fatalf("Error loading heap page from buffer.")
	}

	iter, iter2 := page.tupleIter(), page2.tupleIter()
	if iter == nil {
		t.Fatalf("iter was nil.")
	}
	if iter2 == nil {
		t.Fatalf("iter2 was nil.")
	}

	findEqCount := func(t0 *Tuple, iter3 func() (*Tuple, error)) int {
		cnt := 0
		for tup, _ := iter3(); tup != nil; tup, _ = iter3() {
			if t0.equals(tup) {
				cnt += 1
			}
		}
		return cnt
	}

	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		if findEqCount(tup, page.tupleIter()) != findEqCount(tup, page2.tupleIter()) {
			t.Errorf("Serialization / deserialization doesn't result in identical heap page.")
		}
	}
}

func TestHeapPageBufferLen(t *testing.T) {
	td, _, _, hf, _, _ := makeTestVars(t)
	page, err := newHeapPage(&td, 0, hf)
	if err != nil {
		t.Fatalf(err.Error())
	}
	free := page.getNumSlots()

	for i := 0; i < free-1; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)
	}

	buf, _ := page.toBuffer()

	if buf.Len() != PageSize {
		t.Fatalf("HeapPage.toBuffer returns buffer of unexpected size;  NOTE:  This error may be OK, but many implementations that don't write full pages break.")
	}
}

func TestHeapPageFreeList(t *testing.T) {
	rq := require.New(t)
	free := NewFreeList(133)

	rq.Equal(133, int(free.bitmap.Len()), "Expected free list to have 133 slots")

	// should be empty initially
	rq.False(free.IsFull())

	// Test get slot
	for i := 0; i < 133; i++ {
		rq.False(free.IsFull(), "Expected false, i=%d", i)
		slot := free.GetSlot()
		rq.Equal(i, slot, "Expected slot %d to be %d", i, slot)
		rq.True(free.Test(slot), "Expected slot %d to be marked as used", slot)
	}

	// should be full now
	rq.True(free.IsFull(), "Expected free list to be full after 133 slots")

	// test release
	free.ReleaseSlot(32)
	rq.False(free.IsFull())
	rq.False(free.Test(32), "Expected slot 32 to be released")

	// should get what we released
	slot := free.GetSlot()
	rq.Equal(32, slot)

	free.ReleaseSlot(1)
	free.ReleaseSlot(15)

	// test write to buf
	buf := bytes.NewBuffer(nil)

	n, err := free.WriteTo(buf)
	rq.NoError(err)
	expected := 8 * (divide(133, 64) + 1)
	rq.Equal(int64(expected), n, "Expected 17 bytes written to buffer")

	// should deserialize correctly
	f2 := NewFreeList(0)
	r, err := f2.ReadFrom(buf)
	rq.NoError(err)
	rq.Equal(expected, r)
	rq.True(free.bitmap.Equal(f2.bitmap))
	rq.EqualValues(free.available, f2.available, "Expected available slots to match after deserialization")
}
