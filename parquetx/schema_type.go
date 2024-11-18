package parquetx

import (
	"reflect"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"
)

type u256BinaryTypeImpl struct {
	parquet.Type
}

var u256BinaryType = u256BinaryTypeImpl{
	Type: parquet.ByteArrayType,
}

func (t u256BinaryTypeImpl) String() string { return "BYTE_ARRAY" }

func (t u256BinaryTypeImpl) Kind() parquet.Kind { return parquet.ByteArray }

func (t u256BinaryTypeImpl) Length() int { return 32 }

func (t u256BinaryTypeImpl) EstimateSize(n int) int { return 32 * n }

func (t u256BinaryTypeImpl) EstimateNumValues(n int) int { return n / 32 }

// // Precision is 78 because largest 256 bits integer (2^256 - 1) has 78 digits when printed to string
// var binaryDecimalLogicalType = &format.LogicalType{Decimal: &format.DecimalType{Precision: 76, Scale: 0}}

// func (t u256BinaryTypeImpl) LogicalType() *format.LogicalType { return binaryDecimalLogicalType }

var u256FixedType = u256FixedTypeImpl{
	Type: fixed32ByteArrayType,
}

type u256FixedTypeImpl struct {
	parquet.Type
}

var fixed32ByteArrayType = parquet.FixedLenByteArrayType(32)

func (t u256FixedTypeImpl) String() string { return "FIXED_LEN_BYTE_ARRAY(32)" }

func (t u256FixedTypeImpl) Kind() parquet.Kind { return parquet.FixedLenByteArray }

func (t u256FixedTypeImpl) Length() int { return 32 }

func (t u256FixedTypeImpl) EstimateSize(n int) int { return 32 * n }

func (t u256FixedTypeImpl) EstimateNumValues(n int) int { return n / 32 }

// // Precision is 78 because largest 256 bits integer (2^256 - 1) has 78 digits when printed to string
// var fixedDecimalLogicalType = &format.LogicalType{Decimal: &format.DecimalType{Precision: 78, Scale: 0}}

// func (t u256FixedTypeImpl) LogicalType() *format.LogicalType { return fixedDecimalLogicalType }

var _ parquet.Type = testImpl{}

type testImpl struct {
}

// AssignValue implements parquet.Type.
func (t testImpl) AssignValue(dst reflect.Value, src parquet.Value) error {
	panic("unimplemented")
}

// ColumnOrder implements parquet.Type.
func (t testImpl) ColumnOrder() *format.ColumnOrder {
	panic("unimplemented")
}

// Compare implements parquet.Type.
func (t testImpl) Compare(a parquet.Value, b parquet.Value) int {
	panic("unimplemented")
}

// ConvertValue implements parquet.Type.
func (t testImpl) ConvertValue(val parquet.Value, typ parquet.Type) (parquet.Value, error) {
	panic("unimplemented")
}

// ConvertedType implements parquet.Type.
func (t testImpl) ConvertedType() *deprecated.ConvertedType {
	panic("unimplemented")
}

// Decode implements parquet.Type.
func (t testImpl) Decode(dst encoding.Values, src []byte, enc encoding.Encoding) (encoding.Values, error) {
	panic("unimplemented")
}

// Encode implements parquet.Type.
func (t testImpl) Encode(dst []byte, src encoding.Values, enc encoding.Encoding) ([]byte, error) {
	panic("unimplemented")
}

// EstimateDecodeSize implements parquet.Type.
func (t testImpl) EstimateDecodeSize(numValues int, src []byte, enc encoding.Encoding) int {
	panic("unimplemented")
}

// EstimateNumValues implements parquet.Type.
func (t testImpl) EstimateNumValues(size int) int {
	panic("unimplemented")
}

// EstimateSize implements parquet.Type.
func (t testImpl) EstimateSize(numValues int) int {
	panic("unimplemented")
}

// Kind implements parquet.Type.
func (t testImpl) Kind() parquet.Kind {
	panic("unimplemented")
}

// Length implements parquet.Type.
func (t testImpl) Length() int {
	panic("unimplemented")
}

// LogicalType implements parquet.Type.
func (t testImpl) LogicalType() *format.LogicalType {
	panic("unimplemented")
}

// NewColumnBuffer implements parquet.Type.
func (t testImpl) NewColumnBuffer(columnIndex int, numValues int) parquet.ColumnBuffer {
	panic("unimplemented")
}

// NewColumnIndexer implements parquet.Type.
func (t testImpl) NewColumnIndexer(sizeLimit int) parquet.ColumnIndexer {
	panic("unimplemented")
}

// NewDictionary implements parquet.Type.
func (t testImpl) NewDictionary(columnIndex int, numValues int, data encoding.Values) parquet.Dictionary {
	panic("unimplemented")
}

// NewPage implements parquet.Type.
func (t testImpl) NewPage(columnIndex int, numValues int, data encoding.Values) parquet.Page {
	panic("unimplemented")
}

// NewValues implements parquet.Type.
func (t testImpl) NewValues(values []byte, offsets []uint32) encoding.Values {
	panic("unimplemented")
}

// PhysicalType implements parquet.Type.
func (t testImpl) PhysicalType() *format.Type {
	panic("unimplemented")
}

// String implements parquet.Type.
func (t testImpl) String() string {
	panic("unimplemented")
}
