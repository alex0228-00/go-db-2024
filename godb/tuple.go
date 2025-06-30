package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

func (t DBType) String() string {
	switch t {
	case IntType:
		return "int"
	case StringType:
		return "string"
	}
	return "unknown"
}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

func (f1 FieldType) equals(f2 *FieldType) bool {
	return f1.Fname == f2.Fname && f1.TableQualifier == f2.TableQualifier && f1.Ftype == f2.Ftype
}

func (f1 FieldType) copy() FieldType {
	return FieldType{
		Fname:          f1.Fname,
		TableQualifier: f1.TableQualifier,
		Ftype:          f1.Ftype,
	}
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}

	for i := 0; i < len(d1.Fields); i++ {
		f1 := d1.Fields[i]
		f2 := d2.Fields[i]
		if !f1.equals(&f2) {
			return false
		}
	}
	return true
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	newFields := make([]FieldType, 0, len(td.Fields))

	for _, f := range td.Fields {
		newFields = append(newFields, f.copy())
	}
	return &TupleDesc{
		Fields: newFields,
	}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	td1 := desc.copy()
	td2 := desc2.copy()

	td1.Fields = append(td1.Fields, td2.Fields...)
	return td1
}

// ================== Tuple Methods ======================

// Interface for tuple field values
type DBValue interface {
	EvalPred(v DBValue, op BoolOp) bool
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	for i, td := range t.Desc.Fields {
		switch td.Ftype {
		case IntType:
			val := t.Fields[i].(IntField).Value
			if err := binary.Write(b, binary.LittleEndian, val); err != nil {
				return fmt.Errorf("error writing int field %s: %w", td.Fname, err)
			}
		case StringType:
			str := t.Fields[i].(StringField).Value

			assert(len(str) <= StringLength, "string field %s exceeds maximum length of %d", td.Fname, StringLength)

			if n, err := b.Write([]byte(str)); err != nil {
				return fmt.Errorf("error writing string field %s: %w", td.Fname, err)
			} else if n < StringLength {
				// pad with zeros
				padding := make([]byte, StringLength-n)
				if _, err := b.Write(padding); err != nil {
					return fmt.Errorf("error writing padding for string field %s: %w", td.Fname, err)
				}
			}
		}
	}

	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	t := &Tuple{
		Desc:   *desc.copy(),
		Fields: make([]DBValue, 0, len(desc.Fields)),
	}

	for _, td := range desc.Fields {
		switch td.Ftype {
		case IntType:
			var val int64
			if err := binary.Read(b, binary.LittleEndian, &val); err != nil {
				return nil, fmt.Errorf("error reading int field %s: %w", td.Fname, err)
			}
			t.Fields = append(t.Fields, IntField{Value: val})
		case StringType:
			strBytes := make([]byte, StringLength)
			if n, err := b.Read(strBytes); err != nil {
				return nil, fmt.Errorf("error reading string field %s: %w", td.Fname, err)
			} else if n < StringLength {
				return nil, fmt.Errorf("error reading string field %s: expected %d bytes, got %d", td.Fname, StringLength, n)
			}

			for i := len(strBytes) - 1; i >= 0; i-- {
				if strBytes[i] != 0 {
					strBytes = strBytes[:i+1] // trim trailing zeros
					break
				}
			}

			t.Fields = append(t.Fields, StringField{Value: string(strBytes)})
		default:
			return nil, fmt.Errorf("unsupported field type %s for field %s", td.Ftype, td.Fname)
		}
	}

	return t, nil
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	if !t1.Desc.equals(&t2.Desc) {
		return false
	}

	for i := 0; i < len(t1.Desc.Fields); i++ {
		f1 := t1.Desc.Fields[i]
		f2 := t2.Desc.Fields[i]
		if !f1.equals(&f2) {
			return false
		}

		v1 := t1.Fields[i]
		v2 := t2.Fields[i]
		if !v1.EvalPred(v2, OpEq) {
			return false
		}
	}

	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2
// appended to t1. The new tuple should have a correct TupleDesc that is created
// by merging the descriptions of the two input tuples.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	if t1 == nil {
		return t2
	}
	if t2 == nil {
		return t1
	}

	t := &Tuple{
		Desc:   *t1.Desc.merge(&t2.Desc),
		Fields: make([]DBValue, 0, len(t1.Fields)+len(t2.Fields)),
	}

	t.Fields = append(t.Fields, t1.Fields...)
	t.Fields = append(t.Fields, t2.Fields...)
	return t
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
//
// Note that EvalExpr uses the [Tuple.project] method, so you will need
// to implement projection before testing compareField.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	result1, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, fmt.Errorf("error evaluating expression on first tuple: %w", err)
	}
	result2, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, fmt.Errorf("error evaluating expression on second tuple: %w", err)
	}

	if result1.EvalPred(result2, OpLt) {
		return OrderedLessThan, nil
	}
	if result1.EvalPred(result2, OpEq) {
		return OrderedEqual, nil
	}
	if result1.EvalPred(result2, OpGt) {
		return OrderedGreaterThan, nil
	}
	return OrderedEqual, fmt.Errorf("unexpected comparison result for field %s", field.GetExprType().Fname)
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	ret := &Tuple{
		Desc: TupleDesc{
			Fields: make([]FieldType, 0, len(fields)),
		},
		Fields: make([]DBValue, 0, len(fields)),
	}

	add := func(i int) error {
		f := t.Desc.Fields[i].copy() // copy the field type to avoid modifying the original
		ret.Desc.Fields = append(ret.Desc.Fields, f)
		switch v := t.Fields[i].(type) {
		case IntField:
			ret.Fields = append(ret.Fields, IntField{Value: v.Value})
		case StringField:
			ret.Fields = append(ret.Fields, StringField{Value: v.Value})
		default:
			return fmt.Errorf("unsupported field type %s for field %s", f.Ftype, f.Fname)
		}
		return nil
	}

	for _, target := range fields {
		backup := -1

		for i, f := range t.Desc.Fields {
			if f.Fname == target.Fname && f.Ftype == target.Ftype {
				backup = i
				if f.TableQualifier == target.TableQualifier {
					break
				}
			}
		}

		if backup != -1 {
			// If no exact match was found, use the first match found
			if err := add(backup); err != nil {
				return nil, err
			}
		} else {
			// If no match was found at all, return an error
			return nil, GoDBError{
				IncompatibleTypesError,
				fmt.Sprintf("field %s.%s not found in tuple", target.TableQualifier, target.Fname),
			}
		}
	}

	return ret, nil
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {
	var buf bytes.Buffer
	t.writeTo(&buf)
	return buf.String()
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = strconv.FormatInt(f.Value, 10)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr
}
