// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import "github.com/hanchuanchuan/goInception/types"

// node is the struct implements node interface except for Accept method.
// Node implementations should embed it in.
type node struct {
	text   string
	offset int
}

// SetOriginTextPosition implements Node interface.
func (n *node) SetOriginTextPosition(offset int) {
	n.offset = offset
}

// OriginTextPosition implements Node interface.
func (n *node) OriginTextPosition() int {
	return n.offset
}

// SetText implements Node interface.
func (n *node) SetText(text string) {
	n.text = text
}

// Text implements Node interface.
func (n *node) Text() string {
	return n.text
}

// stmtNode implements StmtNode interface.
// Statement implementations should embed it in.
type stmtNode struct {
	node
}

// statement implements StmtNode interface.
func (sn *stmtNode) statement() {}

// ddlNode implements DDLNode interface.
// DDL implementations should embed it in.
type ddlNode struct {
	stmtNode
}

// ddlStatement implements DDLNode interface.
func (dn *ddlNode) ddlStatement() {}

// dmlNode is the struct implements DMLNode interface.
// DML implementations should embed it in.
type dmlNode struct {
	stmtNode
}

// dmlStatement implements DMLNode interface.
func (dn *dmlNode) dmlStatement() {}

// exprNode is the struct implements Expression interface.
// Expression implementations should embed it in.
type exprNode struct {
	node
	types.Datum
	Type types.FieldType
	flag uint64
}

// SetDatum implements ExprNode interface.
func (en *exprNode) SetDatum(datum types.Datum) {
	en.Datum = datum
}

// GetDatum implements ExprNode interface.
func (en *exprNode) GetDatum() *types.Datum {
	return &en.Datum
}

// TexprNode is exported for parser driver.
type TexprNode = exprNode

// SetType implements ExprNode interface.
func (en *exprNode) SetType(tp *types.FieldType) {
	en.Type = *tp
}

// GetType implements ExprNode interface.
func (en *exprNode) GetType() *types.FieldType {
	return &en.Type
}

// SetFlag implements ExprNode interface.
func (en *exprNode) SetFlag(flag uint64) {
	en.flag = flag
}

// GetFlag implements ExprNode interface.
func (en *exprNode) GetFlag() uint64 {
	return en.flag
}

type funcNode struct {
	exprNode
}

// functionExpression implements FunctionNode interface.
func (fn *funcNode) functionExpression() {}

type resultSetNode struct {
	resultFields []*ResultField
}
