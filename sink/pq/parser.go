package pq

import (
	"fmt"
	"regexp"
	"strings"
)

type ParserError struct {
	Query   string
	Message string
}

func (e *ParserError) Error() string {
	return fmt.Sprintf("%s on %q", e.Message, e.Query)
}

var fieldRegex = regexp.MustCompile(`[^.\[\]]+`)

func Parse(in string) (*Query, error) {
	if !strings.HasPrefix(in, ".") {
		return nil, newParserError(in, "query must start with current object accessor '.'")
	}

	exprs := []Expression{&CurrentAccess{}}

	fieldName := fieldRegex.FindString(in)
	if fieldName == "" {
		return nil, newParserError(in, "query must have field name after accessor '.'")
	}

	exprs = append(exprs, &FieldAccess{Name: fieldName})

	if strings.HasSuffix(in, "[]") {
		exprs = append(exprs, &ArrayAccess{})
	}

	return &Query{
		Elements: exprs,
	}, nil
}

func newParserError(in string, reason string) *ParserError {
	return &ParserError{Query: in, Message: reason}
}
