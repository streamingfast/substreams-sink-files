package parquetx

import (
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

var fixed32ByteArrayType = parquet.FixedLenByteArrayType(32)

// Decimal76Type is a 76 digits precision with a 0 scale decimal type that we use by default for UINT256/INT256 values.
// Sadly, 76 precision does **not** cover the full precision range of a UINT256/INT256, but it's the closest we can get
// with working support out of the box.
var decimal76Type = decimal76TypeImpl{
	Type: fixed32ByteArrayType,
}

type decimal76TypeImpl struct {
	parquet.Type
}

var fixedDecimalLogicalType = &format.LogicalType{Decimal: &format.DecimalType{Precision: 76, Scale: 0}}

func (t decimal76TypeImpl) LogicalType() *format.LogicalType { return fixedDecimalLogicalType }
