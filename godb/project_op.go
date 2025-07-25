package godb

import (
	"fmt"
)

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	// You may want to add additional fields here
	selected map[any]struct{}
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	op := &Project{
		selectFields: selectFields,
		outputNames:  outputNames,
		child:        child,
	}
	if distinct {
		op.selected = make(map[any]struct{})
	}
	assert(len(selectFields) == len(outputNames), "selectFields and outputNames must be of the same length")
	return op, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	td := &TupleDesc{}

	for i, expr := range p.selectFields {
		ft := expr.GetExprType()
		ft.Fname = p.outputNames[i]
		td.Fields = append(td.Fields, ft)
	}
	return td

}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, fmt.Errorf("error getting child iterator: %w", err)
	}
	return func() (*Tuple, error) {
		for {
			t, err := iter()
			if err != nil {
				return nil, fmt.Errorf("error iterating child: %w", err)
			}
			if t == nil {
				return nil, nil // no more tuples
			}

			ret := &Tuple{
				Desc:   *p.Descriptor(),
				Fields: make([]DBValue, 0, len(p.selectFields)),
			}

			for _, expr := range p.selectFields {
				val, err := expr.EvalExpr(t)
				if err != nil {
					return nil, fmt.Errorf("error evaluating expression: %w", err)
				}
				ret.Fields = append(ret.Fields, val)
			}

			if p.selected != nil {
				// Check for distinct tuples
				key := ret.tupleKey()
				if _, exists := p.selected[key]; exists {
					continue // Skip duplicate tuple
				}
				p.selected[key] = struct{}{} // Mark this tuple as seen
			}
			return ret, nil
		}
	}, nil
}
