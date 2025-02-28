package parquetx

import (
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type PathSegment struct {
	FromField string
	Message   protoreflect.Message
}

// LLM context generated comment below (GPT 4o):
//
// In Dremel encoding, when dealing with nested optional fields of type message, the definition
// level and repetition level should be treated in a specific order to correctly represent the
// structure of the data. Here is the order in which they should be treated:
//
// Repetition Level: This level indicates the number of repeated fields between the current field
// and the root of the repeated field. It should be handled first to correctly represent the
// repeated structure of the data.
//
// Definition Level: This level indicates the number of optional fields between the current field
// and the root of the optional field. It should be handled after the repetition level to correctly
// represent the presence or absence of optional fields.
//
// When encoding nested optional fields of type message, you need to increment the definition level
// for each optional field that is present. If a repeated field is encountered, the repetition level
// should be incremented accordingly.
type recursionContext struct {
	parents []PathSegment
	logger  *zap.Logger
	tracer  logging.Tracer

	repeatingLastIndex int

	// We use same underlying type as parquet.Value
	columnIndex     int16
	repetitionLevel byte
	definitionLevel byte
}

func newRecursionContext(root protoreflect.Message, logger *zap.Logger, tracer logging.Tracer) *recursionContext {
	return &recursionContext{
		parents: []PathSegment{{Message: root}},
		logger:  logger,
		tracer:  tracer,
	}
}

func (p *recursionContext) EnterOptional() {
	if p.tracer.Enabled() {
		p.logger.Debug("entering optional field",
			zap.Int("definition_level", int(p.definitionLevel)),
			zap.String("change", "definition_level++"),
		)
	}
	p.definitionLevel++
}

func (p *recursionContext) ExitOptional() {
	if p.tracer.Enabled() {
		p.logger.Debug("exiting optional field",
			zap.Int("definition_level", int(p.definitionLevel)),
			zap.String("change", "definition_level--"),
		)
	}
	p.definitionLevel--
}

func (p *recursionContext) StartRepeated(count int) {
	if p.tracer.Enabled() {
		p.logger.Debug("repeating field",
			zap.Int("count", count),
			zap.Int("definition_level", int(p.definitionLevel)),
			zap.String("change", "definition_level++"),
		)
	}
	p.repeatingLastIndex = count - 1
	p.definitionLevel++
}

func (p *recursionContext) RepeatedIterationCompleted(i int) {
	if i == 0 {
		p.firstRepeatedComplete()
	} else if i == p.repeatingLastIndex {
		p.lastRepeatedComplete()
	}
}

func (p *recursionContext) firstRepeatedComplete() {
	if p.tracer.Enabled() {
		p.logger.Debug("first repeated field complete",
			zap.Int("repetition_level", int(p.repetitionLevel)),
			zap.String("change", "repetition_level++"),
		)
	}
	p.repeatingLastIndex = 0
	p.repetitionLevel++
}

func (p *recursionContext) lastRepeatedComplete() {
	if p.tracer.Enabled() {
		p.logger.Debug("last repeated field complete",
			zap.Int("repetition_level", int(p.repetitionLevel)),
			zap.Int("definition_level", int(p.definitionLevel)),
			zap.String("change", "repetition_level--, definition_level--"),
		)
	}
	p.repetitionLevel--
	p.definitionLevel--
}

func (p *recursionContext) EnterNested(fromField string, message protoreflect.Message) {
	if p.tracer.Enabled() {
		p.logger.Debug("entering nested message",
			zap.String("message", string(message.Descriptor().FullName())),
			zap.Int("definition_level", int(p.definitionLevel)),
			zap.String("change", "definition_level++"),
		)
	}

	p.EnterRepeatedNested(fromField, message)
	p.definitionLevel++
}

// EnterRepeatedNested is called when we are entering a repeated nested message, same as EnterNested,
// but we don't increment the definition level.
func (p *recursionContext) EnterRepeatedNested(fromField string, message protoreflect.Message) {
	p.parents = append(p.parents, PathSegment{FromField: fromField, Message: message})
}

func (p *recursionContext) ExitNested() {
	if p.tracer.Enabled() {
		p.logger.Debug("exiting nested message",
			zap.Int("definition_level", int(p.definitionLevel)),
			zap.String("change", "definition_level--"),
		)
	}
	p.definitionLevel--
	p.ExitRepeatedNested()
}

// ExitRepeatedNested is called when we are done with the repeated nested message, same as
// ExitNested, but we don't decrement the definition level.
func (p *recursionContext) ExitRepeatedNested() {
	p.parents = p.parents[:len(p.parents)-1]
}

func (p *recursionContext) Path() string {
	out := make([]string, len(p.parents))
	for i, m := range p.parents {
		out[i] = string(m.Message.Descriptor().Name())
		if m.FromField != "" {
			out[i] = "." + m.FromField + " " + out[i]
		}
	}

	return strings.Join(out, " -> ")
}

func (p *recursionContext) FieldPath(field protoreflect.FieldDescriptor) string {
	return p.Path() + " -> ." + string(field.Name())
}

func (p *recursionContext) String() string {
	return p.Path()
}

var _ valueLeveler = (*recursionContext)(nil)

// NullValue implements valueLeveler.
func (p *recursionContext) NullValue() parquet.Value {
	return parquet.NullValue().Level(int(p.repetitionLevel), int(p.definitionLevel), int(p.columnIndex))
}

// Level implements valueLeveler.
func (p *recursionContext) Level(value parquet.Value) parquet.Value {
	if p.tracer.Enabled() {
		p.logger.Debug("setting level on value",
			zap.Stringer("value", (*zapParquetValue)(&value)),
			zap.Int("repetition_level", int(p.repetitionLevel)),
			zap.Int("definition_level", int(p.definitionLevel)),
			zap.Int("column_index", int(p.columnIndex)),
		)
	}

	return value.Level(int(p.repetitionLevel), int(p.definitionLevel), int(p.columnIndex))
}

type zapParquetValue parquet.Value

func (v *zapParquetValue) String() string {
	if v == nil {
		return "nil"
	}

	value := (*parquet.Value)(v)
	if value.Kind() == parquet.ByteArray {
		return fmt.Sprintf("ByteArray(%d bytes)", len(value.ByteArray()))
	}

	if value.Kind() == parquet.FixedLenByteArray {
		return fmt.Sprintf("FixedLenByteArray(%d bytes)", len(value.ByteArray()))
	}

	return value.String()
}
