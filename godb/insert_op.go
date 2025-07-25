package godb

import "fmt"

type InsertOp struct {
	insertFile DBFile
	child      Operator
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{
		insertFile: insertFile,
		child:      child,
	}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{"count", "", IntType},
		},
	}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	n := 0
	iter, err := iop.child.Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("error getting child iterator: %w", err)
	}
	return func() (*Tuple, error) {
		for {
			tup, err := iter()
			if err != nil {
				return nil, fmt.Errorf("error iterating child: %w", err)
			}
			if tup == nil {
				break // no more tuples
			}
			err = iop.insertFile.insertTuple(tup, tid)
			if err != nil {
				return nil, fmt.Errorf("error inserting tuple: %w", err)
			}
			n++
		}
		return &Tuple{
			Desc:   *iop.Descriptor(),
			Fields: []DBValue{IntField{int64(n)}},
		}, nil
	}, nil
}
