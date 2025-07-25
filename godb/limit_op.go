package godb

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child, lim}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	limitTuple, _ := l.limitTups.EvalExpr(nil)
	n := limitTuple.(IntField).Value

	return func() (*Tuple, error) {
		if n == 0 {
			return nil, nil
		}
		n--

		return iter()
	}, nil
}
