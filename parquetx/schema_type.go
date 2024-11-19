package parquetx

import (
	"github.com/parquet-go/parquet-go"
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
