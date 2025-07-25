package godb

import (
	"fmt"
)

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	baseAggState
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	a.baseAggState = baseAggState{
		handleInsert: func(state *baseAggState, val DBValue) {
			switch v := val.(type) {
			case IntField:
				if sum, ok := state.state.(IntField); ok {
					state.state = IntField{sum.Value + v.Value}
				}
			case StringField:
				if sum, ok := state.state.(StringField); ok {
					state.state = StringField{sum.Value + v.Value}
				}
			default:
				panic(fmt.Sprintf("SumAggState.AddTuple: unsupported type %T", v))
			}
		},
	}
	return a.baseAggState.Init(alias, expr)
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	sum *SumAggState
	n   int
}

func (a *AvgAggState) Copy() AggState {
	return &AvgAggState{
		sum: a.sum.Copy().(*SumAggState),
		n:   a.n,
	}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	a.sum = &SumAggState{}
	if err := a.sum.Init(alias, expr); err != nil {
		return fmt.Errorf("AvgAggState.Init: %w", err)
	}
	a.n = 0
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	a.sum.AddTuple(t)
	a.n++
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	return a.sum.GetTupleDesc()
}

func (a *AvgAggState) Finalize() *Tuple {
	t := a.sum.Finalize()
	if a.n == 0 {
		return t
	}

	switch v := t.Fields[0].(type) {
	case IntField:
		t.Fields[0] = IntField{v.Value / int64(a.n)}
	default:
		panic(fmt.Sprintf("AvgAggState.Finalize: unsupported type %T", v))
	}
	return t
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	baseAggState
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	a.baseAggState = baseAggState{
		handleInsert: func(state *baseAggState, val DBValue) {
			if state.state.EvalPred(val, OpLt) {
				state.state = val
			}
		},
	}
	return a.baseAggState.Init(alias, expr)
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	baseAggState
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	a.baseAggState = baseAggState{
		handleInsert: func(state *baseAggState, val DBValue) {
			if state.state.EvalPred(val, OpGt) {
				state.state = val
			}
		},
	}
	return a.baseAggState.Init(alias, expr)
}

type baseAggState struct {
	state DBValue
	alias string
	expr  Expr

	handleInsert func(state *baseAggState, val DBValue)
}

func (a *baseAggState) Copy() AggState {
	return &baseAggState{a.state, a.alias, a.expr, a.handleInsert}
}

func (a *baseAggState) Init(alias string, expr Expr) error {
	a.state = nil
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *baseAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	assert(err == nil, "SumAggState.AddTuple: EvalExpr failed")

	if a.state == nil {
		a.state = val
	} else {
		a.handleInsert(a, val)
	}
}

func (a *baseAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{a.alias, "", a.expr.GetExprType().Ftype},
		},
	}
}

func (a *baseAggState) Finalize() *Tuple {
	return &Tuple{
		Desc:   *a.GetTupleDesc(),
		Fields: []DBValue{a.state},
	}
}
