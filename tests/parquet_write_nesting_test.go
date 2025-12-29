package tests

import (
	"testing"

	pbtesting "github.com/streamingfast/substreams-sink-files/v2/internal/pb/tests"
	"google.golang.org/protobuf/proto"
)

func testParquetWriteNestedCases(t *testing.T) {
	testParquetWriteNestedCompleteCases(t)
	testParquetWriteNestedCasesSynthetic(t)
}

// testParquetWriteNestedCompleteCases tests against a single message with different schema
// and instances.
func testParquetWriteNestedCompleteCases(t *testing.T) {
	type GoFlattenedOperation struct {
		Id    string `parquet:"id" db:"id"`
		Token string `parquet:"token" db:"token"`
	}

	type GoTokenMetadata struct {
		Address  string  `parquet:"address" db:"address"`
		Symbol   *string `parquet:"symbol" db:"symbol"`
		Decimals string  `parquet:"decimals" db:"decimals"`
	}

	type GoFlattenedMessage struct {
		Number     *string                 `parquet:"number" db:"number"`
		Id         string                  `parquet:"id" db:"id"`
		Success    bool                    `parquet:"success" db:"success"`
		Memo       *string                 `parquet:"memo" db:"memo"`
		Operations []*GoFlattenedOperation `parquet:"operations" db:"operations"`
		Metadata   []*GoTokenMetadata      `parquet:"metadata" db:"metadata"`
		Provider   *string                 `parquet:"provider" db:"provider"`
	}

	runCases(t, []parquetWriterCase[GoFlattenedMessage]{
		{
			name:        "flattened message with required fields only",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.FlattenedMessage{
					Id:      "txn-001",
					Success: true,
				},
			},
			expectedRows: map[string][]GoFlattenedMessage{
				"flattened": {
					GoFlattenedMessage{
						Number:     nil,
						Id:         "txn-001",
						Success:    true,
						Memo:       nil,
						Operations: []*GoFlattenedOperation{},
						Metadata:   []*GoTokenMetadata{},
						Provider:   nil,
					},
				},
			},
		},
		{
			name:        "flattened message with all optional fields populated",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.FlattenedMessage{
					Number:   ptr("12345"),
					Id:       "txn-002",
					Success:  false,
					Memo:     ptr("Transaction failed"),
					Provider: ptr("MetaMask"),
				},
			},
			expectedRows: map[string][]GoFlattenedMessage{
				"flattened": {
					GoFlattenedMessage{
						Number:     ptr("12345"),
						Id:         "txn-002",
						Success:    false,
						Memo:       ptr("Transaction failed"),
						Operations: []*GoFlattenedOperation{},
						Metadata:   []*GoTokenMetadata{},
						Provider:   ptr("MetaMask"),
					},
				},
			},
		},
		{
			name:        "flattened message with single operation",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.FlattenedMessage{
					Id:      "txn-003",
					Success: true,
					Operations: []*pbtesting.FlattenedOperation{
						{
							Id:    "op-001",
							Token: "ETH",
						},
					},
				},
			},
			expectedRows: map[string][]GoFlattenedMessage{
				"flattened": {
					GoFlattenedMessage{
						Number:  nil,
						Id:      "txn-003",
						Success: true,
						Memo:    nil,
						Operations: []*GoFlattenedOperation{
							{
								Id:    "op-001",
								Token: "ETH",
							},
						},
						Metadata: []*GoTokenMetadata{},
						Provider: nil,
					},
				},
			},
		},
		{
			name:        "flattened message with multiple operations",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.FlattenedMessage{
					Id:      "txn-004",
					Success: true,
					Operations: []*pbtesting.FlattenedOperation{
						{
							Id:    "op-001",
							Token: "ETH",
						},
						{
							Id:    "op-002",
							Token: "USDC",
						},
					},
				},
			},
			expectedRows: map[string][]GoFlattenedMessage{
				"flattened": {
					GoFlattenedMessage{
						Number:  nil,
						Id:      "txn-004",
						Success: true,
						Memo:    nil,
						Operations: []*GoFlattenedOperation{
							{
								Id:    "op-001",
								Token: "ETH",
							},
							{
								Id:    "op-002",
								Token: "USDC",
							},
						},
						Metadata: []*GoTokenMetadata{},
						Provider: nil,
					},
				},
			},
		},
		{
			name:        "flattened message with single metadata",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.FlattenedMessage{
					Id:      "txn-005",
					Success: true,
					Metadata: []*pbtesting.TokenMetadata{
						{
							Address:  "0x1234567890abcdef",
							Symbol:   ptr("ETH"),
							Decimals: "18",
						},
					},
				},
			},
			expectedRows: map[string][]GoFlattenedMessage{
				"flattened": {
					GoFlattenedMessage{
						Number:     nil,
						Id:         "txn-005",
						Success:    true,
						Memo:       nil,
						Operations: []*GoFlattenedOperation{},
						Metadata: []*GoTokenMetadata{
							{
								Address:  "0x1234567890abcdef",
								Symbol:   ptr("ETH"),
								Decimals: "18",
							},
						},
						Provider: nil,
					},
				},
			},
		},
		{
			name:        "flattened message with multiple metadata entries",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.FlattenedMessage{
					Id:      "txn-006",
					Success: true,
					Metadata: []*pbtesting.TokenMetadata{
						{
							Address:  "0x1234567890abcdef",
							Symbol:   ptr("ETH"),
							Decimals: "18",
						},
						{
							Address:  "0xa0b86991c31cc0c1b471482d20e857c0d645c7bc",
							Symbol:   ptr("USDC"),
							Decimals: "6",
						},
					},
				},
			},
			expectedRows: map[string][]GoFlattenedMessage{
				"flattened": {
					GoFlattenedMessage{
						Number:     nil,
						Id:         "txn-006",
						Success:    true,
						Memo:       nil,
						Operations: []*GoFlattenedOperation{},
						Metadata: []*GoTokenMetadata{
							{
								Address:  "0x1234567890abcdef",
								Symbol:   ptr("ETH"),
								Decimals: "18",
							},
							{
								Address:  "0xa0b86991c31cc0c1b471482d20e857c0d645c7bc",
								Symbol:   ptr("USDC"),
								Decimals: "6",
							},
						},
						Provider: nil,
					},
				},
			},
		},
		{
			name:        "flattened message with operations and metadata",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.FlattenedMessage{
					Number:  ptr("67890"),
					Id:      "txn-007",
					Success: true,
					Memo:    ptr("Complex transaction"),
					Operations: []*pbtesting.FlattenedOperation{
						{
							Id:    "op-001",
							Token: "ETH",
						},
						{
							Id:    "op-002",
							Token: "USDC",
						},
					},
					Metadata: []*pbtesting.TokenMetadata{
						{
							Address:  "0x1234567890abcdef",
							Symbol:   ptr("ETH"),
							Decimals: "18",
						},
						{
							Address:  "0xa0b86991c31cc0c1b471482d20e857c0d645c7bc",
							Decimals: "6",
						},
					},
					Provider: ptr("Uniswap"),
				},
			},
			expectedRows: map[string][]GoFlattenedMessage{
				"flattened": {
					GoFlattenedMessage{
						Number:  ptr("67890"),
						Id:      "txn-007",
						Success: true,
						Memo:    ptr("Complex transaction"),
						Operations: []*GoFlattenedOperation{
							{
								Id:    "op-001",
								Token: "ETH",
							},
							{
								Id:    "op-002",
								Token: "USDC",
							},
						},
						Metadata: []*GoTokenMetadata{
							{
								Address:  "0x1234567890abcdef",
								Symbol:   ptr("ETH"),
								Decimals: "18",
							},
							{
								Address: "0xa0b86991c31cc0c1b471482d20e857c0d645c7bc",
								// Quirks from parquet-go library for now, looking into the case.
								Symbol:   ptr(""),
								Decimals: "6",
							},
						},
						Provider: ptr("Uniswap"),
					},
				},
			},
		},
	})
}

type GoNested struct {
	Value string `parquet:"value" db:"value"`
}

// testParquetWriteNestedCasesOriginal tests some synthetic cases with different flavor.
func testParquetWriteNestedCasesSynthetic(t *testing.T) {

	type GoRowNestedMessage struct {
		Nested *GoNested `parquet:"nested" db:"nested"`
	}

	runCases(t, []parquetWriterCase[GoRowNestedMessage]{
		{
			name:        "protobuf table with nested message field, one level deep",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumnNestedMessage{
					Nested: &pbtesting.Nested{
						Value: "abc-0",
					},
				},
			},
			expectedRows: map[string][]GoRowNestedMessage{
				"rows": {
					GoRowNestedMessage{
						Nested: &GoNested{
							Value: "abc-0",
						},
					},
				},
			},
		},
	})

	type GoRowRepeatedNestedMessage struct {
		Nested []*GoNested `parquet:"nested" db:"nested"`
	}

	runCases(t, []parquetWriterCase[GoRowRepeatedNestedMessage]{
		{
			name:        "protobuf table with repeating nested message field, one level deep, empty",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumnRepeatedNestedMessage{
					Nested: []*pbtesting.Nested{},
				},
			},
			expectedRows: map[string][]GoRowRepeatedNestedMessage{
				"rows": {
					GoRowRepeatedNestedMessage{
						Nested: []*GoNested{},
					},
				},
			},
		},
		{
			name:        "protobuf table with repeating nested message field, one level deep, one element",
			onlyDrivers: []string{"parquet-go"},
			outputModules: []proto.Message{
				&pbtesting.RowColumnRepeatedNestedMessage{
					Nested: []*pbtesting.Nested{{
						Value: "abc-0",
					}},
				},
			},
			expectedRows: map[string][]GoRowRepeatedNestedMessage{
				"rows": {
					GoRowRepeatedNestedMessage{
						Nested: []*GoNested{
							{
								Value: "abc-0",
							},
						},
					},
				},
			},
		},
	})
}
