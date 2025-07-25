package godb

import (
	"fmt"
	"sort"
)

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	// TODO: You may want to add additional fields here
	ascending []bool
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{
		orderBy:   orderByFields,
		child:     child,
		ascending: ascending,
	}, nil
}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	iter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("error getting child iterator: %w", err)
	}
	var tuples []*Tuple = nil
	return func() (*Tuple, error) {
		if tuples == nil {
			sorted, err := o.sort(iter)
			if err != nil {
				return nil, fmt.Errorf("error sorting tuples: %w", err)
			}
			tuples = sorted
		}

		if len(tuples) == 0 {
			return nil, nil
		}
		tuple := tuples[0]
		tuples = tuples[1:] // remove the first tuple for next call
		return tuple, nil
	}, nil
}

func (o *OrderBy) sort(iter func() (*Tuple, error)) ([]*Tuple, error) {
	var ret []*Tuple

	for {
		tup, err := iter()
		if err != nil {
			return nil, fmt.Errorf("error iterating child: %w", err)
		}
		if tup == nil {
			break // no more tuples
		}
		ret = append(ret, tup)
	}
	sort.Slice(ret, func(i, j int) bool {
		for k := 0; k < len(o.orderBy); k++ {
			if ret[i].Fields[k].EvalPred(ret[j].Fields[k], OpEq) {
				continue
			}
			if o.ascending[k] {
				if ret[i].Fields[k].EvalPred(ret[j].Fields[k], OpLt) {
					return true
				}
			} else {
				if ret[i].Fields[k].EvalPred(ret[j].Fields[k], OpGt) {
					return true
				}
			}
			break
		}
		return false
	})
	return ret, nil
}
