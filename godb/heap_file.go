package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	fromFile string
	td       *TupleDesc
	n        atomic.Int64

	bufPool *BufferPool
	full    sync.Map
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	hf := &HeapFile{}

	hf.bufPool = bp
	hf.fromFile = fromFile
	hf.td = td

	stat, err := os.Stat(fromFile)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("NewHeapFile: error checking file %s: %v", fromFile, err)
	}

	var n int64
	if os.IsNotExist(err) {
		n = 0
	} else {
		n = stat.Size() / int64(PageSize)
	}
	hf.n.Store(n)
	return hf, nil
}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	// TODO: some code goes here
	return "" //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	return int(f.n.Load())
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

	tid := NewTID()
	bp := f.bufPool
	bp.BeginTransaction(tid)

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
		if err := f.insertTuple(&newT, tid); err != nil {
			return fmt.Errorf("LoadFromCSV: error inserting tuple %d (%s): %v", cnt, line, err)
		}
	}

	bp.CommitTransaction(tid)
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
		if pageNo > f.NumPages()+1 {
			return nil, fmt.Errorf("readPage: pageNo %d is greater than number of pages %d", pageNo, f.NumPages())
		}
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

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.td

}

func (f *HeapFile) createNewPage() (*heapPage, error) {
	f.n.Add(1)
	hp, err := newHeapPage(f.Descriptor(), int(f.n.Load()), f)
	if err != nil {
		return nil, fmt.Errorf("createNewPage: error creating new heap page: %v", err)
	}
	hp.dirty = true
	return hp, nil
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

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("flushPage: error getting file info for %s: %v", f.fromFile, err)
	}

	size := hp.pageNo * PageSize
	if size > int(info.Size()) {
		if err := file.Truncate(int64(size)); err != nil {
			return fmt.Errorf("flushPage: error truncating file %s to %d bytes: %v", f.fromFile, size, err)
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
