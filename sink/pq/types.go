package pq

import "fmt"

//go:generate go-enum -f=$GOFILE --marshal --names

// ENUM(
//
//	Current
//	Field
//	Array
//
// )
type ExpressionKind uint

type Expression interface {
	Kind() ExpressionKind
	String() string
}

type Query struct {
	Elements []Expression
}

type CurrentAccess struct {
}

func (e *CurrentAccess) Kind() ExpressionKind {
	return ExpressionKindCurrent
}

func (e *CurrentAccess) String() string {
	return "."
}

type FieldAccess struct {
	Name string
}

func (e *FieldAccess) Kind() ExpressionKind {
	return ExpressionKindField
}

func (e *FieldAccess) String() string {
	return fmt.Sprintf("Field(%s)", e.Name)
}

type ArrayAccess struct {
}

func (e *ArrayAccess) Kind() ExpressionKind {
	return ExpressionKindArray
}

func (e *ArrayAccess) String() string {
	return "."
}
