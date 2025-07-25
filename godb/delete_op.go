package godb

import (
	"fmt"
)

type DeleteOp struct {
	deleteFile DBFile
	child      Operator
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{
		deleteFile: deleteFile,
		child:      child,
	}
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{"count", "", IntType},
		},
	}

}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	n := 0
	iter, err := dop.child.Iterator(tid)
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
			err = dop.deleteFile.deleteTuple(tup, tid)
			if err != nil {
				return nil, fmt.Errorf("error deleting tuple: %w", err)
			}
			n++
		}
		return &Tuple{
			Desc:   *dop.Descriptor(),
			Fields: []DBValue{IntField{int64(n)}},
		}, nil
	}, nil
}
