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

package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hanchuanchuan/goInception/mysql"
	"github.com/hanchuanchuan/goInception/types"
	"github.com/hanchuanchuan/goInception/util/auth"
	"github.com/hanchuanchuan/goInception/util/hack"
	"github.com/pingcap/errors" // SchemaState is the state for schema elements.
	"github.com/pingcap/tipb/go-tipb"
)

type SchemaState byte

const (
	// StateNone means this schema element is absent and can't be used.
	StateNone SchemaState = iota
	// StateDeleteOnly means we can only delete items for this schema element.
	StateDeleteOnly
	// StateWriteOnly means we can use any write operation on this schema element,
	// but outer can't read the changed data.
	StateWriteOnly
	// StateWriteReorganization means we are re-organizing whole data after write only state.
	StateWriteReorganization
	// StateDeleteReorganization means we are re-organizing whole data after delete only state.
	StateDeleteReorganization
	// StatePublic means this schema element is ok for all write and read operations.
	StatePublic
)

// String implements fmt.Stringer interface.
func (s SchemaState) String() string {
	switch s {
	case StateDeleteOnly:
		return "delete only"
	case StateWriteOnly:
		return "write only"
	case StateWriteReorganization:
		return "write reorganization"
	case StateDeleteReorganization:
		return "delete reorganization"
	case StatePublic:
		return "public"
	default:
		return "none"
	}
}

// ColumnInfo provides meta data describing of a table column.
type ColumnInfo struct {
	ID                  int64               `json:"id"`
	Name                CIStr               `json:"name"`
	Offset              int                 `json:"offset"`
	OriginDefaultValue  interface{}         `json:"origin_default"`
	DefaultValue        interface{}         `json:"default"`
	DefaultValueBit     []byte              `json:"default_bit"`
	GeneratedExprString string              `json:"generated_expr_string"`
	GeneratedStored     bool                `json:"generated_stored"`
	Dependences         map[string]struct{} `json:"dependences"`
	types.FieldType     `json:"type"`
	State               SchemaState `json:"state"`
	Comment             string      `json:"comment"`
}

// Clone clones ColumnInfo.
func (c *ColumnInfo) Clone() *ColumnInfo {
	nc := *c
	return &nc
}

// IsGenerated returns true if the column is generated column.
func (c *ColumnInfo) IsGenerated() bool {
	return len(c.GeneratedExprString) != 0
}

// SetDefaultValue sets the default value.
func (c *ColumnInfo) SetDefaultValue(value interface{}) error {
	c.DefaultValue = value
	if c.Tp == mysql.TypeBit {
		// For mysql.TypeBit type, the default value storage format must be a string.
		// Other value such as int must convert to string format first.
		// The mysql.TypeBit type supports the null default value.
		if value == nil {
			return nil
		}
		if v, ok := value.(string); ok {
			c.DefaultValueBit = []byte(v)
			return nil
		}
		return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
	}
	return nil
}

// GetDefaultValue gets the default value of the column.
// Default value use to stored in DefaultValue field, but now,
// bit type default value will store in DefaultValueBit for fix bit default value decode/encode bug.
func (c *ColumnInfo) GetDefaultValue() interface{} {
	if c.Tp == mysql.TypeBit && c.DefaultValueBit != nil {
		return hack.String(c.DefaultValueBit)
	}
	return c.DefaultValue
}

// FindColumnInfo finds ColumnInfo in cols by name.
func FindColumnInfo(cols []*ColumnInfo, name string) *ColumnInfo {
	name = strings.ToLower(name)
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}

	return nil
}

// ExtraHandleID is the column ID of column which we need to append to schema to occupy the handle's position
// for use of execution phase.
const ExtraHandleID = -1

// ExtraHandleName is the name of ExtraHandle Column.
var ExtraHandleName = NewCIStr("_tidb_rowid")

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	ID      int64  `json:"id"`
	Name    CIStr  `json:"name"`
	Charset string `json:"charset"`
	Collate string `json:"collate"`
	// Columns are listed in the order in which they appear in the schema.
	Columns     []*ColumnInfo `json:"cols"`
	Indices     []*IndexInfo  `json:"index_info"`
	ForeignKeys []*FKInfo     `json:"fk_info"`
	State       SchemaState   `json:"state"`
	PKIsHandle  bool          `json:"pk_is_handle"`
	Comment     string        `json:"comment"`
	AutoIncID   int64         `json:"auto_inc_id"`
	MaxColumnID int64         `json:"max_col_id"`
	MaxIndexID  int64         `json:"max_idx_id"`
	// UpdateTS is used to record the timestamp of updating the table's schema information.
	// These changing schema operations don't include 'truncate table' and 'rename table'.
	UpdateTS uint64 `json:"update_timestamp"`
	// OldSchemaID :
	// Because auto increment ID has schemaID as prefix,
	// We need to save original schemaID to keep autoID unchanged
	// while renaming a table from one database to another.
	// TODO: Remove it.
	// Now it only uses for compatibility with the old version that already uses this field.
	OldSchemaID int64 `json:"old_schema_id,omitempty"`

	// ShardRowIDBits specify if the implicit row ID is sharded.
	ShardRowIDBits uint64

	Partition *PartitionInfo `json:"partition"`

	Compression   string `json:"compression"`
	TempTableType `json:"temp_table_type"`
}

type TempTableType byte

const (
	TempTableNone TempTableType = iota
	TempTableGlobal
	TempTableLocal
)

func (t TempTableType) String() string {
	switch t {
	case TempTableGlobal:
		return "global"
	case TempTableLocal:
		return "local"
	default:
		return ""
	}
}

// TableLockType is the type of the table lock.
type TableLockType byte

const (
	TableLockNone TableLockType = iota
	// TableLockRead means the session with this lock can read the table (but not write it).
	// Multiple sessions can acquire a READ lock for the table at the same time.
	// Other sessions can read the table without explicitly acquiring a READ lock.
	TableLockRead
	// TableLockReadLocal is not supported.
	TableLockReadLocal
	// TableLockWrite means only the session with this lock has write/read permission.
	// Only the session that holds the lock can access the table. No other session can access it until the lock is released.
	TableLockWrite
	// TableLockWriteLocal means the session with this lock has write/read permission, and the other session still has read permission.
	TableLockWriteLocal
)

func (t TableLockType) String() string {
	switch t {
	case TableLockNone:
		return "NONE"
	case TableLockRead:
		return "READ"
	case TableLockReadLocal:
		return "READ LOCAL"
	case TableLockWriteLocal:
		return "WRITE LOCAL"
	case TableLockWrite:
		return "WRITE"
	}
	return ""
}

// GetPartitionInfo returns the partition information.
func (t *TableInfo) GetPartitionInfo() *PartitionInfo {
	if t.Partition != nil && t.Partition.Enable {
		return t.Partition
	}
	return nil
}

// GetUpdateTime gets the table's updating time.
func (t *TableInfo) GetUpdateTime() time.Time {
	return TSConvert2Time(t.UpdateTS)
}

// GetDBID returns the schema ID that is used to create an allocator.
// TODO: Remove it after removing OldSchemaID.
func (t *TableInfo) GetDBID(dbID int64) int64 {
	if t.OldSchemaID != 0 {
		return t.OldSchemaID
	}
	return dbID
}

// Clone clones TableInfo.
func (t *TableInfo) Clone() *TableInfo {
	nt := *t
	nt.Columns = make([]*ColumnInfo, len(t.Columns))
	nt.Indices = make([]*IndexInfo, len(t.Indices))
	nt.ForeignKeys = make([]*FKInfo, len(t.ForeignKeys))

	for i := range t.Columns {
		nt.Columns[i] = t.Columns[i].Clone()
	}

	for i := range t.Indices {
		nt.Indices[i] = t.Indices[i].Clone()
	}

	for i := range t.ForeignKeys {
		nt.ForeignKeys[i] = t.ForeignKeys[i].Clone()
	}

	return &nt
}

// GetPkName will return the pk name if pk exists.
func (t *TableInfo) GetPkName() CIStr {
	if t.PKIsHandle {
		for _, colInfo := range t.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				return colInfo.Name
			}
		}
	}
	return CIStr{}
}

// GetPkColInfo gets the ColumnInfo of pk if exists.
// Make sure PkIsHandle checked before call this method.
func (t *TableInfo) GetPkColInfo() *ColumnInfo {
	for _, colInfo := range t.Columns {
		if mysql.HasPriKeyFlag(colInfo.Flag) {
			return colInfo
		}
	}
	return nil
}

// Cols returns the columns of the table in public state.
func (t *TableInfo) Cols() []*ColumnInfo {
	publicColumns := make([]*ColumnInfo, len(t.Columns))
	maxOffset := -1
	for _, col := range t.Columns {
		if col.State != StatePublic {
			continue
		}
		publicColumns[col.Offset] = col
		if maxOffset < col.Offset {
			maxOffset = col.Offset
		}
	}
	return publicColumns[0 : maxOffset+1]
}

// NewExtraHandleColInfo mocks a column info for extra handle column.
func NewExtraHandleColInfo() *ColumnInfo {
	colInfo := &ColumnInfo{
		ID:   ExtraHandleID,
		Name: ExtraHandleName,
	}
	colInfo.Flag = mysql.PriKeyFlag
	colInfo.Tp = mysql.TypeLonglong
	colInfo.Flen, colInfo.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	return colInfo
}

// ColumnIsInIndex checks whether c is included in any indices of t.
func (t *TableInfo) ColumnIsInIndex(c *ColumnInfo) bool {
	for _, index := range t.Indices {
		for _, column := range index.Columns {
			if column.Name.L == c.Name.L {
				return true
			}
		}
	}
	return false
}

// ViewAlgorithm is VIEW's SQL AlGORITHM characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/view-algorithms.html
type ViewAlgorithm int

const (
	AlgorithmUndefined ViewAlgorithm = iota
	AlgorithmMerge
	AlgorithmTemptable
)

func (v *ViewAlgorithm) String() string {
	switch *v {
	case AlgorithmMerge:
		return "MERGE"
	case AlgorithmTemptable:
		return "TEMPTABLE"
	case AlgorithmUndefined:
		return "UNDEFINED"
	default:
		return "UNDEFINED"
	}
}

// ViewSecurity is VIEW's SQL SECURITY characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/create-view.html
type ViewSecurity int

const (
	SecurityDefiner ViewSecurity = iota
	SecurityInvoker
)

func (v *ViewSecurity) String() string {
	switch *v {
	case SecurityInvoker:
		return "INVOKER"
	case SecurityDefiner:
		return "DEFINER"
	default:
		return "DEFINER"
	}
}

// ViewCheckOption is VIEW's WITH CHECK OPTION clause part.
// See https://dev.mysql.com/doc/refman/5.7/en/view-check-option.html
type ViewCheckOption int

const (
	CheckOptionLocal ViewCheckOption = iota
	CheckOptionCascaded
)

func (v *ViewCheckOption) String() string {
	switch *v {
	case CheckOptionLocal:
		return "LOCAL"
	case CheckOptionCascaded:
		return "CASCADED"
	default:
		return "CASCADED"
	}
}

// ViewInfo provides meta data describing a DB view.
type ViewInfo struct {
	Algorithm   ViewAlgorithm      `json:"view_algorithm"`
	Definer     *auth.UserIdentity `json:"view_definer"`
	Security    ViewSecurity       `json:"view_security"`
	SelectStmt  string             `json:"view_select"`
	CheckOption ViewCheckOption    `json:"view_checkoption"`
	Cols        []CIStr            `json:"view_cols"`
}

// PartitionIndexType is the type for partition index
type PartitionIndexType int

// Partition index types.
const (
	PartitionIndexTypeInvalid PartitionIndexType = iota
	PartitionIndexTypeLocal
	PartitionIndexTypeGlobal
)

func (t PartitionIndexType) String() string {
	switch t {
	case PartitionIndexTypeLocal:
		return "LOCAL"
	case PartitionIndexTypeGlobal:
		return "GLOBAL"
	default:
		return ""
	}
}

// PartitionType is the type for PartitionInfo
type PartitionType int

// Partition types.
const (
	PartitionTypeRange       PartitionType = 1
	PartitionTypeHash                      = 2
	PartitionTypeList                      = 3
	PartitionTypeKey                       = 4
	PartitionTypeSystemTime                = 5
	PartitionTypeMM                        = 6
	PartitionTypeDD                        = 7
	PartitionTypeWEEK                      = 8
	PartitionTypeMMDD                      = 9
	PartitionTypeYYYYMM                    = 10
	PartitionTypeYYYYWEEK                  = 11
	PartitionTypeYYYYDD                    = 12
	PartitionTypeYYYYMMOPT                 = 13
	PartitionTypeYYYYWEEKOPT               = 14
	PartitionTypeYYYYDDOPT                 = 15
	PartitionTypeUNIHASH                   = 16
	PartitionTypeRIGHTSHIFT                = 17
)

func (p PartitionType) String() string {
	switch p {
	case PartitionTypeRange:
		return "RANGE"
	case PartitionTypeHash:
		return "HASH"
	case PartitionTypeList:
		return "LIST"
	case PartitionTypeKey:
		return "KEY"
	case PartitionTypeSystemTime:
		return "SYSTEM_TIME"
	case PartitionTypeMM:
		return "MM"
	case PartitionTypeDD:
		return "DD"
	case PartitionTypeWEEK:
		return "WEEK"
	case PartitionTypeMMDD:
		return "MMDD"
	case PartitionTypeYYYYMM:
		return "YYYYMM"
	case PartitionTypeYYYYWEEK:
		return "YYYYWEEK"
	case PartitionTypeYYYYDD:
		return "YYYYDD"
	case PartitionTypeYYYYMMOPT:
		return "YYYYMM_OPT"
	case PartitionTypeYYYYWEEKOPT:
		return "YYYYWEEK_OPT"
	case PartitionTypeYYYYDDOPT:
		return "YYYYDD_OPT"
	case PartitionTypeUNIHASH:
		return "UNI_HASH"
	case PartitionTypeRIGHTSHIFT:
		return "RIGHT_SHIFT"
	default:
		return ""
	}

}

// PartitionInfo provides table partition info.
type PartitionInfo struct {
	Type    PartitionType `json:"type"`
	Expr    string        `json:"expr"`
	Columns []CIStr       `json:"columns"`

	// User may already creates table with partition but table partition is not
	// yet supported back then. When Enable is true, write/read need use tid
	// rather than pid.
	Enable bool `json:"enable"`

	Definitions []PartitionDefinition `json:"definitions"`
	Num         uint64                `json:"num"`
}

// GetNameByID gets the partition name by ID.
func (pi *PartitionInfo) GetNameByID(id int64) string {
	for _, def := range pi.Definitions {
		if id == def.ID {
			return def.Name.L
		}
	}
	return ""
}

// PartitionDefinition defines a single partition.
type PartitionDefinition struct {
	ID       int64    `json:"id"`
	Name     CIStr    `json:"name"`
	LessThan []string `json:"less_than"`
	Comment  string   `json:"comment,omitempty"`
}

// IndexColumn provides index column info.
type IndexColumn struct {
	Name   CIStr `json:"name"`   // Index name
	Offset int   `json:"offset"` // Index offset
	// Length of prefix when using column prefix
	// for indexing;
	// UnspecifedLength if not using prefix indexing
	Length int `json:"length"`
}

// Clone clones IndexColumn.
func (i *IndexColumn) Clone() *IndexColumn {
	ni := *i
	return &ni
}

// IndexType is the type of index
type IndexType int

// String implements Stringer interface.
func (t IndexType) String() string {
	switch t {
	case IndexTypeBtree:
		return "BTREE"
	case IndexTypeHash:
		return "HASH"
	case IndexTypeRtree:
		return "RTREE"
	default:
		return ""
	}
}

// IndexTypes
const (
	IndexTypeInvalid IndexType = iota
	IndexTypeBtree
	IndexTypeHash
	IndexTypeRtree
)

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	ID      int64          `json:"id"`
	Name    CIStr          `json:"idx_name"`   // Index name.
	Table   CIStr          `json:"tbl_name"`   // Table name.
	Columns []*IndexColumn `json:"idx_cols"`   // Index columns.
	Unique  bool           `json:"is_unique"`  // Whether the index is unique.
	Primary bool           `json:"is_primary"` // Whether the index is primary key.
	State   SchemaState    `json:"state"`
	Comment string         `json:"comment"`    // Comment
	Tp      IndexType      `json:"index_type"` // Index type: Btree or Hash
}

// Clone clones IndexInfo.
func (index *IndexInfo) Clone() *IndexInfo {
	ni := *index
	ni.Columns = make([]*IndexColumn, len(index.Columns))
	for i := range index.Columns {
		ni.Columns[i] = index.Columns[i].Clone()
	}
	return &ni
}

// HasPrefixIndex returns whether any columns of this index uses prefix length.
func (index *IndexInfo) HasPrefixIndex() bool {
	for _, ic := range index.Columns {
		if ic.Length != types.UnspecifiedLength {
			return true
		}
	}
	return false
}

// PrimaryKeyType is the type of primary key.
// Available values are "clustered", "nonclustered", and ""(default).
type PrimaryKeyType int8

func (p PrimaryKeyType) String() string {
	switch p {
	case PrimaryKeyTypeClustered:
		return "CLUSTERED"
	case PrimaryKeyTypeNonClustered:
		return "NONCLUSTERED"
	default:
		return ""
	}
}

//revive:disable:exported
const (
	PrimaryKeyTypeDefault PrimaryKeyType = iota
	PrimaryKeyTypeClustered
	PrimaryKeyTypeNonClustered
)

// FKInfo provides meta data describing a foreign key constraint.
type FKInfo struct {
	ID       int64       `json:"id"`
	Name     CIStr       `json:"fk_name"`
	RefTable CIStr       `json:"ref_table"`
	RefCols  []CIStr     `json:"ref_cols"`
	Cols     []CIStr     `json:"cols"`
	OnDelete int         `json:"on_delete"`
	OnUpdate int         `json:"on_update"`
	State    SchemaState `json:"state"`
}

// Clone clones FKInfo.
func (fk *FKInfo) Clone() *FKInfo {
	nfk := *fk

	nfk.RefCols = make([]CIStr, len(fk.RefCols))
	nfk.Cols = make([]CIStr, len(fk.Cols))
	copy(nfk.RefCols, fk.RefCols)
	copy(nfk.Cols, fk.Cols)

	return &nfk
}

// DBInfo provides meta data describing a DB.
type DBInfo struct {
	ID      int64        `json:"id"`      // Database ID
	Name    CIStr        `json:"db_name"` // DB name.
	Charset string       `json:"charset"`
	Collate string       `json:"collate"`
	Tables  []*TableInfo `json:"-"` // Tables in the DB.
	State   SchemaState  `json:"state"`
}

// Clone clones DBInfo.
func (db *DBInfo) Clone() *DBInfo {
	newInfo := *db
	newInfo.Tables = make([]*TableInfo, len(db.Tables))
	for i := range db.Tables {
		newInfo.Tables[i] = db.Tables[i].Clone()
	}
	return &newInfo
}

// Copy shallow copies DBInfo.
func (db *DBInfo) Copy() *DBInfo {
	newInfo := *db
	newInfo.Tables = make([]*TableInfo, len(db.Tables))
	copy(newInfo.Tables, db.Tables)
	return &newInfo
}

// CIStr is case insensitive string.
type CIStr struct {
	O string `json:"O"` // Original string.
	L string `json:"L"` // Lower case string.
}

// String implements fmt.Stringer interface.
func (cis CIStr) String() string {
	return cis.O
}

// NewCIStr creates a new CIStr.
func NewCIStr(s string) (cs CIStr) {
	cs.O = s
	cs.L = strings.ToLower(s)
	return
}

// UnmarshalJSON implements the user defined unmarshal method.
// CIStr can be unmarshaled from a single string, so PartitionDefinition.Name
// in this change https://github.com/hanchuanchuan/goInception/pull/6460/files would be
// compatible during TiDB upgrading.
func (cis *CIStr) UnmarshalJSON(b []byte) error {
	type T CIStr
	if err := json.Unmarshal(b, (*T)(cis)); err == nil {
		return nil
	}

	// Unmarshal CIStr from a single string.
	err := json.Unmarshal(b, &cis.O)
	if err != nil {
		return errors.Trace(err)
	}
	cis.L = strings.ToLower(cis.O)
	return nil
}

// ColumnsToProto converts a slice of model.ColumnInfo to a slice of tipb.ColumnInfo.
func ColumnsToProto(columns []*ColumnInfo, pkIsHandle bool) []*tipb.ColumnInfo {
	cols := make([]*tipb.ColumnInfo, 0, len(columns))
	for _, c := range columns {
		col := ColumnToProto(c)
		// TODO: Here `PkHandle`'s meaning is changed, we will change it to `IsHandle` when tikv's old select logic
		// is abandoned.
		if (pkIsHandle && mysql.HasPriKeyFlag(c.Flag)) || c.ID == ExtraHandleID {
			col.PkHandle = true
		} else {
			col.PkHandle = false
		}
		cols = append(cols, col)
	}
	return cols
}

// IndexToProto converts a model.IndexInfo to a tipb.IndexInfo.
func IndexToProto(t *TableInfo, idx *IndexInfo) *tipb.IndexInfo {
	pi := &tipb.IndexInfo{
		TableId: t.ID,
		IndexId: idx.ID,
		Unique:  idx.Unique,
	}
	cols := make([]*tipb.ColumnInfo, 0, len(idx.Columns)+1)
	for _, c := range idx.Columns {
		cols = append(cols, ColumnToProto(t.Columns[c.Offset]))
	}
	if t.PKIsHandle {
		// Coprocessor needs to know PKHandle column info, so we need to append it.
		for _, col := range t.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				colPB := ColumnToProto(col)
				colPB.PkHandle = true
				cols = append(cols, colPB)
				break
			}
		}
	}
	pi.Columns = cols
	return pi
}

// ColumnToProto converts model.ColumnInfo to tipb.ColumnInfo.
func ColumnToProto(c *ColumnInfo) *tipb.ColumnInfo {
	pc := &tipb.ColumnInfo{
		ColumnId:  c.ID,
		Collation: collationToProto(c.FieldType.Collate),
		ColumnLen: int32(c.FieldType.Flen),
		Decimal:   int32(c.FieldType.Decimal),
		Flag:      int32(c.Flag),
		Elems:     c.Elems,
	}
	pc.Tp = int32(c.FieldType.Tp)
	return pc
}

// TODO: update it when more collate is supported.
func collationToProto(c string) int32 {
	v := mysql.CollationNames[c]
	if v == mysql.BinaryCollationID {
		return int32(mysql.BinaryCollationID)
	}
	// We only support binary and utf8_bin collation.
	// Setting other collations to utf8_bin for old data compatibility.
	// For the data created when we didn't enforce utf8_bin collation in create table.
	return int32(mysql.DefaultCollationID)
}

// GetTableColumnID gets a ID of a column with table ID
func GetTableColumnID(tableInfo *TableInfo, col *ColumnInfo) string {
	return fmt.Sprintf("%d_%d", tableInfo.ID, col.ID)
}
