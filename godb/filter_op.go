package godb

import (
	"fmt"
)

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator

	desc *TupleDesc
}

// Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	desc := &TupleDesc{
		Fields: []FieldType{field.GetExprType()},
	}
	return &Filter{op, field, constExpr, child, desc}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	return f.desc
}

// Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := f.child.Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("fail to get iterator: %w", err)
	}
	right, _ := f.right.EvalExpr(nil)
	return func() (*Tuple, error) {
		for {
			tup, err := iter()
			if err != nil {
				return nil, fmt.Errorf("fail to run iter for childern operator: %w", err)
			}
			if tup == nil {
				return nil, nil
			}

			if val, err := f.left.EvalExpr(tup); err != nil {
				return nil, fmt.Errorf("fail to run left EvalExpr: %w", err)
			} else if val.EvalPred(right, f.op) {
				return tup, nil
			}
		}
	}, nil
}
