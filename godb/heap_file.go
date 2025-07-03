package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	bufPool    *BufferPool
	fromFile   string
	td         *TupleDesc
	n          int64
	pageInFile int

	full      map[int]struct{}
	available map[int]struct{}
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	hf := &HeapFile{
		fromFile:  fromFile,
		td:        td,
		bufPool:   bp,
		full:      make(map[int]struct{}),
		available: make(map[int]struct{}),
	}

	stat, err := os.Stat(fromFile)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("NewHeapFile: error checking file %s: %v", fromFile, err)
	}

	if os.IsNotExist(err) {
		hf.n = 0
		hf.pageInFile = 0
	} else {
		hf.n = stat.Size() / int64(PageSize)
		hf.pageInFile = int(hf.n)
	}
	return hf, nil
}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	// TODO: some code goes here
	return "" //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	return int(f.n)
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		f.insertTuple(&newT, tid)

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()

	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	assert(pageNo >= 0, "readPage: pageNo %d is less than 0", pageNo)

	if pageNo > f.NumPages() {
		assert(pageNo == f.NumPages()+1, "readPage: pageNo %d is greater than number of pages %d", pageNo, f.NumPages())
		return f.createNewPage()
	}

	file, err := os.OpenFile(f.fromFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("readPage: error opening file %s: %v", f.fromFile, err)
	}
	defer file.Close()

	offset := int64((pageNo - 1) * PageSize)
	if ret, err := file.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("readPage: error seeking to page %d in file % s: %v", pageNo, f.fromFile, err)
	} else if ret != offset {
		return nil, fmt.Errorf("readPage: expected to seek to offset %d, but got %d", offset, ret)
	}

	buf := make([]byte, PageSize)
	if n, err := file.Read(buf); err != nil {
		return nil, fmt.Errorf("readPage: error reading page %d from file %s : %v", pageNo, f.fromFile, err)
	} else if n != PageSize {
		return nil, fmt.Errorf("readPage: expected to read %d bytes, but read %d bytes", PageSize, n)
	}

	hp, err := newHeapPage(f.Descriptor(), pageNo, f)
	if err != nil {
		return nil, fmt.Errorf("readPage: error creating new heap page: %v", err)
	}

	if err := hp.initFromBuffer(bytes.NewBuffer(buf)); err != nil {
		return nil, fmt.Errorf("readPage: error initializing heap page from buffer: %v", err)
	}
	return hp, nil
}

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
		f.full[hp.pageNo] = struct{}{}
	} else {
		f.available[hp.pageNo] = struct{}{}
	}
	return nil
}

func (f *HeapFile) getFreePage(tid TransactionID, perm RWPerm) (*heapPage, error) {
	steps := []func(tid TransactionID, perm RWPerm) (*heapPage, error){
		f.getPageFromFreeList,
		f.getFreePageFromCurrentPages,
	}

	for _, step := range steps {
		hp, err := step(tid, perm)
		if err != nil {
			return nil, fmt.Errorf("insertTuple: error getting page: %v", err)
		}
		if hp != nil {
			return hp, nil
		}
	}

	return nil, fmt.Errorf("insertTuple: no free page found")
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

func (f *HeapFile) getPageFromFreeList(tid TransactionID, perm RWPerm) (*heapPage, error) {
	if len(f.available) == 0 {
		return nil, nil
	}

	var pageNo int
	for pageNo = range f.available {
		delete(f.available, pageNo)
		break
	}

	hp, err := f.getPage(pageNo, tid, perm)
	if err != nil {
		return nil, fmt.Errorf("getPageFromFreeList: error getting page %d from heap file: %v", pageNo, err)
	}

	assert(!hp.freelist.IsFull(), "getPageFromFreeList: expected free page, got full page %d", pageNo)
	return hp, nil
}

func (f *HeapFile) getFreePageFromCurrentPages(tid TransactionID, perm RWPerm) (*heapPage, error) {
	for i := 1; i <= f.NumPages()+1; i++ {
		if _, ok := f.full[i]; ok {
			continue
		}

		hp, err := f.getPage(i, tid, perm)
		if err != nil {
			return nil, fmt.Errorf("getFreePageFromCurrentPages: error getting page %d from heap file: %v", i, err)
		}

		if !hp.freelist.IsFull() {
			return hp, nil
		} else {
			f.full[i] = struct{}{}
		}
	}
	return nil, nil
}

func (f *HeapFile) createNewPage() (*heapPage, error) {
	f.n++
	hp, err := newHeapPage(f.Descriptor(), int(f.n), f)
	if err != nil {
		return nil, fmt.Errorf("createNewPage: error creating new heap page: %v", err)
	}
	hp.dirty = true
	return hp, nil
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

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	hp, ok := p.(*heapPage)
	assert(ok, "flushPage: expected heapPage, got %T", p)

	file, err := os.OpenFile(f.fromFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("flushPage: error opening file %s: %v", f.fromFile, err)
	}
	defer file.Close()

	if hp.pageNo > f.pageInFile {
		f.pageInFile = hp.pageNo

		if err := file.Truncate(int64(f.pageInFile * PageSize)); err != nil {
			return fmt.Errorf("flushPage: error truncating file %s to %d bytes: %v", f.fromFile, f.pageInFile*PageSize, err)
		}
	}

	offset := (hp.pageNo - 1) * PageSize
	if _, err := file.Seek(int64(offset), 0); err != nil {
		return fmt.Errorf("flushPage: error seeking to page %d in file %s: %v", hp.pageNo, f.fromFile, err)
	}

	buf, err := hp.toBuffer()
	if err != nil {
		return fmt.Errorf("flushPage: error writing freelist to buffer: %v", err)
	}

	if n, err := file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("flushPage: error writing page %d to file %s: %v", hp.pageNo, f.fromFile, err)
	} else if n != PageSize {
		return fmt.Errorf("flushPage: expected to write %d bytes, but wrote %d bytes", PageSize, n)
	}
	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.td

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

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{FileName: f.fromFile, PageNo: pgNo}
}
