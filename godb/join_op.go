package godb

import (
	"fmt"
)

type EqualityJoin struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator // Operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should
	// use (only required for optional exercise).
	maxBufferSize int
}

// Constructor for a join of integer expressions.
//
// Returns an error if either the left or right expression is not an integer.
func NewJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin, error) {
	return &EqualityJoin{leftField, rightField, &left, &right, maxBufferSize}, nil
}

// Return a TupleDesc for this join. The returned descriptor should contain the
// union of the fields in the descriptors of the left and right operators.
//
// HINT: use [TupleDesc.merge].
func (hj *EqualityJoin) Descriptor() *TupleDesc {
	return (*hj.left).Descriptor().merge((*hj.right).Descriptor())
}

// Join operator implementation. This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
//
// HINT: When implementing the simple nested loop join, you should keep in mind
// that you only iterate through the left iterator once (outer loop) but iterate
// through the right iterator once for every tuple in the left iterator (inner
// loop).
//
// HINT: You can use [Tuple.joinTuples] to join two tuples.
//
// OPTIONAL EXERCISE: the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out. To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	var (
		iter func() (*Tuple, error)
	)

	leftIter, err := (*joinOp.left).Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("Error getting left iterator: %v", err)
	}

	return func() (*Tuple, error) {
		for {
			if iter == nil {
				records, err := batchLoad(joinOp.maxBufferSize, leftIter, joinOp.leftField)
				if err != nil {
					return nil, fmt.Errorf("Error loading left batch: %v", err)
				}
				if len(records) == 0 {
					return nil, nil
				}

				iter, err = joinOp.iterBatch(tid, records)
				if err != nil {
					return nil, fmt.Errorf("Error getting batch iterator: %v", err)
				}
			}

			t, err := iter()
			if err != nil {
				return nil, fmt.Errorf("Error iterating: %v", err)
			}
			if t == nil {
				iter = nil
				continue
			}
			return t, nil
		}
	}, nil
}

func (joinOp *EqualityJoin) iterBatch(tid TransactionID, batch map[DBValue][]*Tuple) (func() (*Tuple, error), error) {
	rightIter, err := (*joinOp.right).Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("Error getting right iterator: %v", err)
	}

	var (
		left  []*Tuple
		right *Tuple
	)

	return func() (*Tuple, error) {
		for {
			if len(left) == 0 {
				t, err := rightIter()
				if err != nil {
					return nil, fmt.Errorf("Error iterating right: %v", err)
				} else if t == nil {
					return nil, nil
				}

				value, err := joinOp.rightField.EvalExpr(t)
				if err != nil {
					return nil, fmt.Errorf("Error evaluating right field: %v", err)
				}

				tuples, ok := batch[value]
				if !ok {
					continue
				}

				assert(len(tuples) > 0, "Expected at least one left tuple for value %v", value)
				left = tuples
				right = t
			}

			cur := left[0]
			left = left[1:]
			return joinTuples(cur, right), nil

		}
	}, nil
}

func batchLoad(n int, iter func() (*Tuple, error), expr Expr) (map[DBValue][]*Tuple, error) {
	ret := make(map[DBValue][]*Tuple, n)
	for i := 0; i < n; i++ {
		tup, err := iter()
		if err != nil {
			return nil, fmt.Errorf("Error iterating: %v", err)
		}
		if tup == nil {
			break
		}
		val, err := expr.EvalExpr(tup)
		if err != nil {
			return nil, fmt.Errorf("Error evaluating field: %v", err)
		}
		ret[val] = append(ret[val], tup)
	}
	return ret, nil
}
