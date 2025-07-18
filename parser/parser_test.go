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

package parser

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/model"
	"github.com/hanchuanchuan/goInception/mysql"
	"github.com/hanchuanchuan/goInception/terror"
	"github.com/hanchuanchuan/goInception/util/charset"
	"github.com/hanchuanchuan/goInception/util/testleak"
	. "github.com/pingcap/check"
	// "github.com/pingcap/errors"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testParserSuite{})

type testParserSuite struct {
}

func (s *testParserSuite) TestSimple(c *C) {
	defer testleak.AfterTest(c)()
	parser := New()

	reservedKws := []string{
		"add", "all", "alter", "analyze", "and", "as", "asc", "between", "bigint",
		"binary", "blob", "both", "by", "cascade", "case", "change", "character", "check", "collate",
		"column", "constraint", "convert", "create", "cross", "current_date", "current_time",
		"current_timestamp", "current_user", "database", "databases", "day_hour", "day_microsecond",
		"day_minute", "day_second", "decimal", "default", "delete", "desc", "describe",
		"distinct", "distinctRow", "div", "double", "drop", "dual", "else", "enclosed", "escaped",
		"exists", "explain", "false", "float", "for", "force", "foreign", "from",
		"fulltext", "grant", "group", "having", "hour_microsecond", "hour_minute",
		"hour_second", "if", "ignore", "in", "index", "infile", "inner", "insert", "int", "into", "integer",
		"interval", "is", "join", "key", "keys", "kill", "leading", "left", "like", "limit", "lines", "load",
		"localtime", "localtimestamp", "lock", "longblob", "longtext", "mediumblob", "maxvalue", "mediumint", "mediumtext",
		"minute_microsecond", "minute_second", "mod", "not", "no_write_to_binlog", "null", "numeric",
		"on", "option", "or", "order", "outer", "partition", "precision", "primary", "procedure", "range", "read", "real",
		"references", "regexp", "rename", "repeat", "replace", "revoke", "restrict", "right", "rlike",
		"schema", "schemas", "second_microsecond", "select", "set", "show", "smallint",
		"starting", "table", "terminated", "then", "tinyblob", "tinyint", "tinytext", "to",
		"trailing", "true", "union", "unique", "unlock", "unsigned",
		"update", "use", "using", "utc_date", "values", "varbinary", "varchar",
		"when", "where", "write", "xor", "year_month", "zerofill",
		"generated", "virtual", "stored", "usage",
		"delayed", "high_priority", "low_priority",
		"rowNumber", "window", "groups", "rows", "over", "cumeDist", "denseRank", "firstValue",
		"lag", "lastValue", "lead", "nthValue", "ntile", "percentRank", "rank", "error",
		// TODO: support the following keywords
		// "with",
	}
	for _, kw := range reservedKws {
		src := fmt.Sprintf("SELECT * FROM db.%s;", kw)
		_, err := parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))

		src = fmt.Sprintf("SELECT * FROM %s.desc", kw)
		_, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))

		src = fmt.Sprintf("SELECT t.%s FROM t", kw)
		_, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))
	}

	// Testcase for unreserved keywords
	unreservedKws := []string{
		"auto_increment", "after", "begin", "bit", "bool", "boolean", "charset", "columns", "commit",
		"date", "datediff", "datetime", "deallocate", "do", "from_days", "end", "engine", "engines", "execute", "first", "full",
		"local", "names", "offset", "password", "prepare", "quick", "rollback", "session", "signed",
		"start", "tables", "tablespace", "text", "time", "timestamp", "tidb", "transaction", "truncate", "unknown",
		"value", "warnings", "year", "now", "substr", "subpartition", "subpartitions", "substring", "mode", "any", "some", "user", "identified",
		"collation", "comment", "avg_row_length", "checksum", "compression", "connection", "key_block_size",
		"max_rows", "min_rows", "national", "row", "quarter", "escape", "grants", "status", "fields", "triggers",
		"delay_key_write", "isolation", "partitions", "repeatable", "committed", "uncommitted", "only", "serializable", "level",
		"curtime", "variables", "dayname", "version", "btree", "hash", "row_format", "dynamic", "fixed", "compressed",
		"compact", "redundant", "1 sql_no_cache", "1 sql_cache", "action", "round",
		"enable", "disable", "reverse", "space", "privileges", "get_lock", "release_lock", "sleep", "no", "greatest", "least",
		"binlog", "hex", "unhex", "function", "indexes", "from_unixtime", "processlist", "events", "less", "than", "timediff",
		"ln", "log", "log2", "log10", "timestampdiff", "pi", "quote", "none", "super", "shared", "exclusive",
		"always", "stats", "stats_meta", "stats_histogram", "stats_buckets", "stats_healthy", "tidb_version", "replication", "slave", "client",
		"max_connections_per_hour", "max_queries_per_hour", "max_updates_per_hour", "max_user_connections", "event", "reload", "routine", "temporary",
		"preceding", "unbounded", "nulls", "respect",
	}
	for _, kw := range unreservedKws {
		src := fmt.Sprintf("SELECT %s FROM tbl;", kw)
		_, err := parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))
	}

	// Testcase for prepared statement
	src := "SELECT id+?, id+? from t;"
	_, err := parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// Testcase for -- Comment and unary -- operator
	src = "CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED); -- foo\nSelect --1 from foo;"
	stmts, _, err := parser.Parse(src, "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 2)

	// Testcase for /*! xx */
	// See http://dev.mysql.com/doc/refman/5.7/en/comments.html
	// Fix: https://github.com/hanchuanchuan/goInception/issues/971
	src = "/*!40101 SET character_set_client = utf8 */;"
	stmts, _, err = parser.Parse(src, "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 1)
	stmt := stmts[0]
	_, ok := stmt.(*ast.SetStmt)
	c.Assert(ok, IsTrue)

	// for issue #2017
	src = "insert into blobtable (a) values ('/*! truncated */');"
	stmt, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	is, ok := stmt.(*ast.InsertStmt)
	c.Assert(ok, IsTrue)
	c.Assert(is.Lists, HasLen, 1)
	c.Assert(is.Lists[0], HasLen, 1)
	c.Assert(is.Lists[0][0].GetDatum().GetString(), Equals, "/*! truncated */")

	// Testcase for CONVERT(expr,type)
	src = "SELECT CONVERT('111', SIGNED);"
	st, err := parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	ss, ok := st.(*ast.SelectStmt)
	c.Assert(ok, IsTrue)
	c.Assert(len(ss.Fields.Fields), Equals, 1)
	cv, ok := ss.Fields.Fields[0].Expr.(*ast.FuncCastExpr)
	c.Assert(ok, IsTrue)
	c.Assert(cv.FunctionType, Equals, ast.CastConvertFunction)

	// for query start with comment
	srcs := []string{
		"/* some comments */ SELECT CONVERT('111', SIGNED) ;",
		"/* some comments */ /*comment*/ SELECT CONVERT('111', SIGNED) ;",
		"SELECT /*comment*/ CONVERT('111', SIGNED) ;",
		"SELECT CONVERT('111', /*comment*/ SIGNED) ;",
		"SELECT CONVERT('111', SIGNED) /*comment*/;",
	}
	for _, src := range srcs {
		st, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil)
		ss, ok = st.(*ast.SelectStmt)
		c.Assert(ok, IsTrue)
	}

	// for issue #961
	src = "create table t (c int key);"
	st, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	cs, ok := st.(*ast.CreateTableStmt)
	c.Assert(ok, IsTrue)
	c.Assert(cs.Cols, HasLen, 1)
	c.Assert(cs.Cols[0].Options, HasLen, 1)
	c.Assert(cs.Cols[0].Options[0].Tp, Equals, ast.ColumnOptionPrimaryKey)

	// for issue #4497
	src = "create table t1(a NVARCHAR(100));"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// for issue 2803
	src = "use quote;"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// issue #4354
	src = "select b'';"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	src = "select B'';"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// src = "select 0b'';"
	// _, err = parser.ParseOneStmt(src, "", "")
	// c.Assert(err, NotNil)

	// for #4909, support numericType `signed` filedOpt.
	src = "CREATE TABLE t(_sms smallint signed, _smu smallint unsigned);"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// for #7371, support NATIONAL CHARACTER
	// reference link: https://dev.mysql.com/doc/refman/5.7/en/charset-national.html
	src = "CREATE TABLE t(c1 NATIONAL CHARACTER(10));"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	src = `CREATE TABLE t(a tinyint signed,
		b smallint signed,
		c mediumint signed,
		d int signed,
		e int1 signed,
		f int2 signed,
		g int3 signed,
		h int4 signed,
		i int8 signed,
		j integer signed,
		k bigint signed,
		l bool signed,
		m boolean signed
		);`

	st, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	ct, ok := st.(*ast.CreateTableStmt)
	c.Assert(ok, IsTrue)
	for _, col := range ct.Cols {
		c.Assert(col.Tp.Flag&mysql.UnsignedFlag, Equals, uint(0))
	}

	// for issue #4006
	src = `insert into tb(v) (select v from tb);`
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
}

type testCase struct {
	src     string
	ok      bool
	restore string
}

type testErrMsgCase struct {
	src string
	err error
}

func (s *testParserSuite) RunTest(c *C, table []testCase, enableWindowFunc bool) {
	parser := New()
	parser.EnableWindowFunc(enableWindowFunc)
	for _, t := range table {
		_, _, err := parser.Parse(t.src, "", "")
		comment := Commentf("source %v", t.src)
		if t.ok {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(err, NotNil, comment)
		}
	}
}

func (s *testParserSuite) RunErrMsgTest(c *C, table []testErrMsgCase) {
	parser := New()
	for _, t := range table {
		_, _, err := parser.Parse(t.src, "", "")
		comment := Commentf("source %v", t.src)
		if t.err != nil {
			c.Assert(terror.ErrorEqual(err, t.err), IsTrue, comment)
		} else {
			c.Assert(err, IsNil, comment)
		}
	}
}

func (s *testParserSuite) TestDMLStmt(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{"", true, ""},
		{";", true, ""},
		{"INSERT INTO foo VALUES (1234)", true, "INSERT INTO `foo` VALUES (1234)"},
		{"INSERT INTO foo VALUES (1234, 5678)", true, "INSERT INTO `foo` VALUES (1234,5678)"},
		{"INSERT INTO t1 (SELECT * FROM t2)", true, "INSERT INTO `t1` (SELECT * FROM `t2`)"},
		{"INSERT INTO t partition (p0) values(1234)", true, "INSERT INTO `t` PARTITION(`p0`) VALUES (1234)"},
		{"REPLACE INTO t partition (p0) values(1234)", true, "REPLACE INTO `t` PARTITION(`p0`) VALUES (1234)"},
		{"INSERT INTO t partition (p0, p1, p2) values(1234)", true, "INSERT INTO `t` PARTITION(`p0`, `p1`, `p2`) VALUES (1234)"},
		{"REPLACE INTO t partition (p0, p1, p2) values(1234)", true, "REPLACE INTO `t` PARTITION(`p0`, `p1`, `p2`) VALUES (1234)"},
		// 15
		{"INSERT INTO foo VALUES (1 || 2)", true, "INSERT INTO `foo` VALUES (1 OR 2)"},
		{"INSERT INTO foo VALUES (1 | 2)", true, "INSERT INTO `foo` VALUES (1|2)"},
		{"INSERT INTO foo VALUES (false || true)", true, "INSERT INTO `foo` VALUES (FALSE OR TRUE)"},
		{"INSERT INTO foo VALUES (bar(5678))", true, "INSERT INTO `foo` VALUES (BAR(5678))"},
		// 20
		{"INSERT INTO foo VALUES ()", true, "INSERT INTO `foo` VALUES ()"},
		{"SELECT * FROM t", true, "SELECT * FROM `t`"},
		{"SELECT * FROM t AS u", true, "SELECT * FROM `t` AS `u`"},
		// 25
		{"SELECT * FROM t, v", true, "SELECT * FROM (`t`) JOIN `v`"},
		{"SELECT * FROM t AS u, v", true, "SELECT * FROM (`t` AS `u`) JOIN `v`"},
		{"SELECT * FROM t, v AS w", true, "SELECT * FROM (`t`) JOIN `v` AS `w`"},
		{"SELECT * FROM t AS u, v AS w", true, "SELECT * FROM (`t` AS `u`) JOIN `v` AS `w`"},
		{"SELECT * FROM foo, bar, foo", true, "SELECT * FROM ((`foo`) JOIN `bar`) JOIN `foo`"},
		// 30
		{"SELECT DISTINCTS * FROM t", false, ""},
		{"SELECT DISTINCT * FROM t", true, "SELECT DISTINCT * FROM `t`"},
		{"SELECT DISTINCTROW * FROM t", true, "SELECT DISTINCT * FROM `t`"},
		{"SELECT ALL * FROM t", true, "SELECT ALL * FROM `t`"},
		{"SELECT DISTINCT ALL * FROM t", false, ""},
		{"SELECT DISTINCTROW ALL * FROM t", false, ""},
		{"INSERT INTO foo (a) VALUES (42)", true, "INSERT INTO `foo` (`a`) VALUES (42)"},
		{"INSERT INTO foo (a,) VALUES (42,)", false, ""},
		// 35
		{"INSERT INTO foo (a,b) VALUES (42,314)", true, "INSERT INTO `foo` (`a`,`b`) VALUES (42,314)"},
		{"INSERT INTO foo (a,b,) VALUES (42,314)", false, ""},
		{"INSERT INTO foo (a,b,) VALUES (42,314,)", false, ""},
		{"INSERT INTO foo () VALUES ()", true, "INSERT INTO `foo` () VALUES ()"},
		{"INSERT INTO foo VALUE ()", true, "INSERT INTO `foo` VALUES ()"},

		// for issue 2402
		{"INSERT INTO tt VALUES (01000001783);", true, "INSERT INTO `tt` VALUES (1000001783)"},
		{"INSERT INTO tt VALUES (default);", true, "INSERT INTO `tt` VALUES (DEFAULT)"},

		{"REPLACE INTO foo VALUES (1 || 2)", true, "REPLACE INTO `foo` VALUES (1 OR 2)"},
		{"REPLACE INTO foo VALUES (1 | 2)", true, "REPLACE INTO `foo` VALUES (1|2)"},
		{"REPLACE INTO foo VALUES (false || true)", true, "REPLACE INTO `foo` VALUES (FALSE OR TRUE)"},
		{"REPLACE INTO foo VALUES (bar(5678))", true, "REPLACE INTO `foo` VALUES (BAR(5678))"},
		{"REPLACE INTO foo VALUES ()", true, "REPLACE INTO `foo` VALUES ()"},
		{"REPLACE INTO foo (a,b) VALUES (42,314)", true, "REPLACE INTO `foo` (`a`,`b`) VALUES (42,314)"},
		{"REPLACE INTO foo (a,b,) VALUES (42,314)", false, ""},
		{"REPLACE INTO foo (a,b,) VALUES (42,314,)", false, ""},
		{"REPLACE INTO foo () VALUES ()", true, "REPLACE INTO `foo` () VALUES ()"},
		{"REPLACE INTO foo VALUE ()", true, "REPLACE INTO `foo` VALUES ()"},
		// 40
		{`SELECT stuff.id
			FROM stuff
			WHERE stuff.value >= ALL (SELECT stuff.value
			FROM stuff)`, true, "SELECT `stuff`.`id` FROM `stuff` WHERE `stuff`.`value`>=ALL (SELECT `stuff`.`value` FROM `stuff`)"},
		{"BEGIN", true, "START TRANSACTION"},
		{"START TRANSACTION", true, "START TRANSACTION"},
		// 45
		{"COMMIT", true, "COMMIT"},
		{"ROLLBACK", true, "ROLLBACK"},
		{`BEGIN;
			INSERT INTO foo VALUES (42, 3.14);
			INSERT INTO foo VALUES (-1, 2.78);
		COMMIT;`, true, "START TRANSACTION; INSERT INTO `foo` VALUES (42,3.14); INSERT INTO `foo` VALUES (-1,2.78); COMMIT"},
		{`BEGIN;
			INSERT INTO tmp SELECT * from bar;
			SELECT * from tmp;
		ROLLBACK;`, true, "START TRANSACTION; INSERT INTO `tmp` SELECT * FROM `bar`; SELECT * FROM `tmp`; ROLLBACK"},

		// table statement
		{"TABLE t", true, "TABLE `t`"},
		{"(TABLE t)", true, "(TABLE `t`)"},
		{"TABLE t1, t2", false, ""},
		{"TABLE t ORDER BY b", true, "TABLE `t` ORDER BY `b`"},
		{"TABLE t LIMIT 3", true, "TABLE `t` LIMIT 3"},
		{"TABLE t ORDER BY b LIMIT 3", true, "TABLE `t` ORDER BY `b` LIMIT 3"},
		{"TABLE t ORDER BY b LIMIT 3 OFFSET 2", true, "TABLE `t` ORDER BY `b` LIMIT 2,3"},
		{"TABLE t ORDER BY b LIMIT 2,3", true, "TABLE `t` ORDER BY `b` LIMIT 2,3"},
		{"INSERT INTO ta TABLE tb", true, "INSERT INTO `ta` TABLE `tb`"},
		{"INSERT INTO t.a TABLE t.b", true, "INSERT INTO `t`.`a` TABLE `t`.`b`"},
		{"REPLACE INTO ta TABLE tb", true, "REPLACE INTO `ta` TABLE `tb`"},
		{"REPLACE INTO t.a TABLE t.b", true, "REPLACE INTO `t`.`a` TABLE `t`.`b`"},
		{"CREATE TABLE t.a TABLE t.b", true, "CREATE TABLE `t`.`a` AS TABLE `t`.`b`"},
		{"CREATE TABLE ta TABLE tb", true, "CREATE TABLE `ta` AS TABLE `tb`"},
		{"CREATE TABLE ta (x INT) TABLE tb", true, "CREATE TABLE `ta` (`x` INT) AS TABLE `tb`"},
		{"CREATE VIEW v AS TABLE t", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS TABLE `t`"},
		{"CREATE VIEW v AS (TABLE t)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (TABLE `t`)"},
		{"SELECT * FROM t1 WHERE a IN (TABLE t2)", true, "SELECT * FROM `t1` WHERE `a` IN (TABLE `t2`)"},

		// values statement
		{"VALUES ROW(1)", true, "VALUES ROW(1)"},
		{"VALUES ROW()", true, "VALUES ROW()"},
		{"VALUES ROW(1, default)", true, "VALUES ROW(1,DEFAULT)"},
		{"VALUES ROW(1), ROW(2,3)", true, "VALUES ROW(1), ROW(2,3)"},
		{"VALUES (1,2)", false, ""},
		{"VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8)", true, "VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8)"},
		{"VALUES ROW(1,s,3.1), ROW(5,y,9.9)", true, "VALUES ROW(1,`s`,3.1), ROW(5,`y`,9.9)"},
		{"VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) LIMIT 3", true, "VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) LIMIT 3"},
		{"VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY a", true, "VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY `a`"},
		{"VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY a LIMIT 2", true, "VALUES ROW(1,-2,3), ROW(5,7,9), ROW(4,6,8) ORDER BY `a` LIMIT 2"},
		{"CREATE TABLE ta VALUES ROW(1)", true, "CREATE TABLE `ta` AS VALUES ROW(1)"},
		{"CREATE TABLE ta AS VALUES ROW(1)", true, "CREATE TABLE `ta` AS VALUES ROW(1)"},
		{"CREATE VIEW a AS VALUES ROW(1)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `a` AS VALUES ROW(1)"},

		// qualified select
		{"SELECT a.b.c FROM t", true, "SELECT `a`.`b`.`c` FROM `t`"},
		{"SELECT a.b.*.c FROM t", false, ""},
		{"SELECT a.b.* FROM t", true, "SELECT `a`.`b`.* FROM `t`"},
		{"SELECT a FROM t", true, "SELECT `a` FROM `t`"},
		{"SELECT a.b.c.d FROM t", false, ""},

		// do statement
		{"DO 1", true, "DO 1"},
		{"DO 1, sleep(1)", true, "DO 1, SLEEP(1)"},
		{"DO 1 from t", false, ""},

		// load data
		// {"load data local infile '/tmp/t.csv' into table t1 fields terminated by ',' optionally enclosed by '\"' ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES"},
		// {"load data infile '/tmp/t.csv' into table t", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t`"},
		// {"load data infile '/tmp/t.csv' into table t character set utf8", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t`"},
		// {"load data infile '/tmp/t.csv' into table t fields terminated by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab'"},
		// {"load data infile '/tmp/t.csv' into table t columns terminated by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab'"},
		// {"load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"},
		// {"load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"},
		// {"load data infile '/tmp/t.csv' into table t lines starting by 'ab'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` LINES STARTING BY 'ab'"},
		// {"load data infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy'"},
		// {"load data infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy'", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy'"},
		// {"load data infile '/tmp/t.csv' into table t terminated by 'xy' fields terminated by 'ab'", false, ""},
		// {"load data local infile '/tmp/t.csv' into table t", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t`"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab'"},
		// {"load data local infile '/tmp/t.csv' into table t columns terminated by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab'"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"},
		// {"load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*'"},
		// {"load data local infile '/tmp/t.csv' into table t lines starting by 'ab'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab'"},
		// {"load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy'"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy'"},
		// {"load data local infile '/tmp/t.csv' into table t terminated by 'xy' fields terminated by 'ab'", false, ""},
		// {"load data infile '/tmp/t.csv' into table t (a,b)", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t` (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t columns terminated by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' enclosed by 'b' escaped by '*' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t lines starting by 'ab' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' lines terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy' (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' LINES TERMINATED BY 'xy' (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t (a,b) fields terminated by 'ab'", false, ""},
		// {"load data local infile '/tmp/t.csv' into table t ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` IGNORE 1 LINES"},
		// {"load data local infile '/tmp/t.csv' into table t ignore -1 lines", false, ""},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' (a,b) ignore 1 lines", false, ""},
		// {"load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy' ignore 1 lines", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` LINES STARTING BY 'ab' TERMINATED BY 'xy' IGNORE 1 LINES"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*' ignore 1 lines (a,b)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '*' IGNORE 1 LINES (`a`,`b`)"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by ''", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY ''"},

		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b'"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' escaped by '' enclosed by 'b'", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY ''"},
		// {"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' escaped by '' enclosed by 'b' SET b = CAST(CONV(MID(@var1, 3, LENGTH(@var1)-3), 2, 10) AS UNSIGNED)", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t` FIELDS TERMINATED BY 'ab' ENCLOSED BY 'b' ESCAPED BY '' SET `b`=CAST(CONV(MID(@`var1`, 3, LENGTH(@`var1`)-3), 2, 10) AS UNSIGNED)"},

		// {"LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @dummy, column2, @dummy, column3)", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`dummy`,`column2`,@`dummy`,`column3`)"},
		// {"LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @var1) SET column2 = @var1/100", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`var1`) SET `column2`=@`var1`/100"},
		// {"LOAD DATA INFILE 'file.txt' INTO TABLE t1 (column1, @var1, @var2) SET column2 = @var1/100, column3 = DEFAULT, column4=CURRENT_TIMESTAMP, column5=@var2+1", true, "LOAD DATA INFILE 'file.txt' INTO TABLE `t1` (`column1`,@`var1`,@`var2`) SET `column2`=@`var1`/100, `column3`=DEFAULT, `column4`=CURRENT_TIMESTAMP(), `column5`=@`var2`+1"},

		// {"LOAD DATA INFILE '/tmp/t.csv' INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA INFILE '/tmp/t.csv' INTO TABLE `t1` FIELDS TERMINATED BY ','"},
		// {"LOAD DATA LOCAL INFILE '/tmp/t.csv' INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ','"},
		// {"LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' IGNORE INTO TABLE `t1` FIELDS TERMINATED BY ','"},
		// {"LOAD DATA LOCAL INFILE '/tmp/t.csv' REPLACE INTO TABLE t1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';", true, "LOAD DATA LOCAL INFILE '/tmp/t.csv' REPLACE INTO TABLE `t1` FIELDS TERMINATED BY ','"},

		// select for update
		{"select * from t for update", true, "SELECT * FROM `t` FOR UPDATE"},
		{"select * from t for share", true, "SELECT * FROM `t` FOR SHARE"},
		{"select * from t for update nowait", true, "SELECT * FROM `t` FOR UPDATE NOWAIT"},
		{"select * from t for update wait 5", true, "SELECT * FROM `t` FOR UPDATE WAIT 5"},
		{"select * from t limit 1 for update wait 11", true, "SELECT * FROM `t` LIMIT 1 FOR UPDATE WAIT 11"},
		{"select * from t for share nowait", true, "SELECT * FROM `t` FOR SHARE NOWAIT"},
		{"select * from t for update skip locked", true, "SELECT * FROM `t` FOR UPDATE SKIP LOCKED"},
		{"select * from t for share skip locked", true, "SELECT * FROM `t` FOR SHARE SKIP LOCKED"},
		{"select * from t lock in share mode", true, "SELECT * FROM `t` FOR SHARE"},
		{"select * from t lock in share mode nowait", false, ""},
		{"select * from t lock in share mode skip locked", false, ""},

		{"select * from t for update of t", true, "SELECT * FROM `t` FOR UPDATE OF `t`"},
		{"select * from t for share of t", true, "SELECT * FROM `t` FOR SHARE OF `t`"},
		{"select * from t for update of t nowait", true, "SELECT * FROM `t` FOR UPDATE OF `t` NOWAIT"},
		{"select * from t for update of t wait 5", true, "SELECT * FROM `t` FOR UPDATE OF `t` WAIT 5"},
		{"select * from t limit 1 for update of t wait 11", true, "SELECT * FROM `t` LIMIT 1 FOR UPDATE OF `t` WAIT 11"},
		{"select * from t for share of t nowait", true, "SELECT * FROM `t` FOR SHARE OF `t` NOWAIT"},
		{"select * from t for update of t skip locked", true, "SELECT * FROM `t` FOR UPDATE OF `t` SKIP LOCKED"},
		{"select * from t for share of t skip locked", true, "SELECT * FROM `t` FOR SHARE OF `t` SKIP LOCKED"},
		// from join
		{"SELECT * from t1, t2, t3", true, "SELECT * FROM ((`t1`) JOIN `t2`) JOIN `t3`"},
		{"select * from t1 join t2 left join t3 on t2.id = t3.id", true, "SELECT * FROM (`t1` JOIN `t2`) LEFT JOIN `t3` ON `t2`.`id`=`t3`.`id`"},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3 on t3.id = t2.id", true, "SELECT * FROM (`t1` RIGHT JOIN `t2` ON `t1`.`id`=`t2`.`id`) LEFT JOIN `t3` ON `t3`.`id`=`t2`.`id`"},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3", false, ""},
		{"select * from t1 join t2 left join t3 using (id)", true, "SELECT * FROM (`t1` JOIN `t2`) LEFT JOIN `t3` USING (`id`)"},
		{"select * from t1 right join t2 using (id) left join t3 using (id)", true, "SELECT * FROM (`t1` RIGHT JOIN `t2` USING (`id`)) LEFT JOIN `t3` USING (`id`)"},
		{"select * from t1 right join t2 using (id) left join t3", false, ""},
		{"select * from t1 natural join t2", true, "SELECT * FROM `t1` NATURAL JOIN `t2`"},
		{"select * from t1 natural right join t2", true, "SELECT * FROM `t1` NATURAL RIGHT JOIN `t2`"},
		{"select * from t1 natural left outer join t2", true, "SELECT * FROM `t1` NATURAL LEFT JOIN `t2`"},
		{"select * from t1 natural inner join t2", false, ""},
		{"select * from t1 natural cross join t2", false, ""},
		{"select * from t3 join t1 join t2 on t1.a=t2.a on t3.b=t2.b", true, "SELECT * FROM `t3` JOIN (`t1` JOIN `t2` ON `t1`.`a`=`t2`.`a`) ON `t3`.`b`=`t2`.`b`"},

		// for straight_join
		{"select * from t1 straight_join t2 on t1.id = t2.id", true, "SELECT * FROM `t1` STRAIGHT_JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		{"select straight_join * from t1 join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		{"select straight_join * from t1 left join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` LEFT JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		{"select straight_join * from t1 right join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` RIGHT JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		{"select straight_join * from t1 straight_join t2 on t1.id = t2.id", true, "SELECT STRAIGHT_JOIN * FROM `t1` STRAIGHT_JOIN `t2` ON `t1`.`id`=`t2`.`id`"},
		// for json_table
		{"SELECT * FROM JSON_TABLE('{\"a\":\"3\"}', '$.*' columns (cc int path '$.c1')) as ts;", true, "SELECT * FROM JSON_TABLE('{\"a\":\"3\"}', '$.*' COLUMNS(cc INT PATH '$.C1')) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (cc for ordinality)) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(cc FOR ORDINALITY)) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (bx int exists path '$.b')) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(BX INT EXISTS PATH '$.b')) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (nested path '$.b[*]' columns(bpath varchar(10) path '$.c', nested path '$.l[*]' columns (lpath varchar(10) path '$')))) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(NESTED PATH '$.b[*]' COLUMNS(bpath VARCHAR(10) PATH '$.c', NESTED PATH '$.l[*]' COLUMNS (lpath VARCHAR(10) PATH '$')))) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (cc int path '$.c1' default '{\"a\":123}' on empty)) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(cc INT PATH '$.C1' DEFAULT '{\"a\":123}' ON EMPTY)) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (cc int path '$.c1' default '{\"a\":123}' on error)) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(cc INT PATH '$.C1' DEFAULT '{\"a\":123}' ON ERROR)) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (cc int path '$.c1' null on empty)) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(cc INT PATH '$.C1' NULL ON EMPTY)) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (cc int path '$.c1' null on error)) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(cc INT PATH '$.C1' NULL ON ERROR)) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (cc int path '$.c1' default '{\"a\":123}' on empty default '{\"a\":123}' on error)) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(cc INT PATH '$.C1' DEFAULT '{\"a\":123}' ON EMPTY DEFAULT '{\"a\":123}' ON ERROR)) AS TS"},
		{"SELECT * FROM JSON_TABLE('[a]', '$.*' columns (cc int path '$.c1' null on empty null on error)) as ts;", true, "SELECT * FROM JSON_TABLE('[A]', '$.*' COLUMNS(cc INT PATH '$.C1' NULL ON EMPTY NULL ON ERROR)) AS TS"},
		// delete statement
		// single table syntax
		{"DELETE from t1", true, "DELETE FROM `t1`"},
		{"DELETE from t1.*", false, ""},
		{"DELETE LOW_priORITY from t1", true, "DELETE LOW_PRIORITY FROM `t1`"},
		{"DELETE quick from t1", true, "DELETE QUICK FROM `t1`"},
		{"DELETE ignore from t1", true, "DELETE IGNORE FROM `t1`"},
		{"DELETE low_priority quick ignore from t1", true, "DELETE LOW_PRIORITY QUICK IGNORE FROM `t1`"},
		{"DELETE FROM t1 WHERE t1.a > 0 ORDER BY t1.a", true, "DELETE FROM `t1` WHERE `t1`.`a`>0 ORDER BY `t1`.`a`"},
		{"delete from t1 where a=26", true, "DELETE FROM `t1` WHERE `a`=26"},
		{"DELETE from t1 where a=1 limit 1", true, "DELETE FROM `t1` WHERE `a`=1 LIMIT 1"},
		{"DELETE FROM t1 WHERE t1.a > 0 ORDER BY t1.a LIMIT 1", true, "DELETE FROM `t1` WHERE `t1`.`a`>0 ORDER BY `t1`.`a` LIMIT 1"},
		{"DELETE FROM x.y z WHERE z.a > 0", true, "DELETE FROM `x`.`y` AS `z` WHERE `z`.`a`>0"},
		{"DELETE FROM t1 AS w WHERE a > 0", true, "DELETE FROM `t1` AS `w` WHERE `a`>0"},

		// multi table syntax: before from
		{"delete low_priority t1, t2 from t1, t2", true, "DELETE LOW_PRIORITY `t1`,`t2` FROM (`t1`) JOIN `t2`"},
		{"delete quick t1, t2 from t1, t2", true, "DELETE QUICK `t1`,`t2` FROM (`t1`) JOIN `t2`"},
		{"delete ignore t1, t2 from t1, t2", true, "DELETE IGNORE `t1`,`t2` FROM (`t1`) JOIN `t2`"},
		{"delete low_priority quick ignore t1, t2 from t1, t2 where t1.a > 5", true, "DELETE LOW_PRIORITY QUICK IGNORE `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`a`>5"},
		{"delete t1, t2 from t1, t2", true, "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2`"},
		{"delete t1, t2 from t1, t2 where t1.a = 1 and t2.b <> 1", true, "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`a`=1 AND `t2`.`b`!=1"},
		{"delete t1 from t1, t2", true, "DELETE `t1` FROM (`t1`) JOIN `t2`"},
		{"delete t2 from t1, t2", true, "DELETE `t2` FROM (`t1`) JOIN `t2`"},
		{"delete t1 from t1", true, "DELETE `t1` FROM `t1`"},
		{"delete t1,t2,t3 from t1, t2, t3", true, "DELETE `t1`,`t2`,`t3` FROM ((`t1`) JOIN `t2`) JOIN `t3`"},
		{"delete t1,t2,t3 from t1, t2, t3 where t3.c < 5 and t1.a = 3", true, "DELETE `t1`,`t2`,`t3` FROM ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t3`.`c`<5 AND `t1`.`a`=3"},
		{"delete t1 from t1, t1 as t2 where t1.b = t2.b and t1.a > t2.a", true, "DELETE `t1` FROM (`t1`) JOIN `t1` AS `t2` WHERE `t1`.`b`=`t2`.`b` AND `t1`.`a`>`t2`.`a`"},
		{"delete t.t1.*,t2 from t1, t2", true, "DELETE `t`.`t1`,`t2` FROM (`t1`) JOIN `t2`"},
		{"DELETE from t1 partition (p0,p1)", true, "DELETE FROM `t1` PARTITION(`p0`, `p1`)"},
		// {"delete t1.*,t2 from t1, t2", true, "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2`"},
		// {"delete t1.*, t2.* from t1, t2", true, "DELETE `t1`,`t2` FROM (`t1`) JOIN `t2`"},
		// {"delete t11.*, t12.* from t11, t12 where t11.a = t12.a and t11.b <> 1", true, "DELETE `t11`,`t12` FROM (`t11`) JOIN `t12` WHERE `t11`.`a`=`t12`.`a` AND `t11`.`b`!=1"},

		// multi table syntax: with using
		{"DELETE quick FROM t1,t2 USING t1,t2", true, "DELETE QUICK FROM `t1`,`t2` USING (`t1`) JOIN `t2`"},
		{"DELETE low_priority ignore FROM t1,t2 USING t1,t2", true, "DELETE LOW_PRIORITY IGNORE FROM `t1`,`t2` USING (`t1`) JOIN `t2`"},
		{"DELETE low_priority quick ignore FROM t1,t2 USING t1,t2", true, "DELETE LOW_PRIORITY QUICK IGNORE FROM `t1`,`t2` USING (`t1`) JOIN `t2`"},
		{"delete ignore t1, t2 from t1 partition (p0,p1), t2", true, "DELETE IGNORE `t1`,`t2` FROM (`t1` PARTITION(`p0`, `p1`)) JOIN `t2`"},
		{"DELETE FROM t1 USING t1 WHERE post='1'", true, "DELETE FROM `t1` USING `t1` WHERE `post`='1'"},
		{"DELETE FROM t1,t2 USING t1,t2", true, "DELETE FROM `t1`,`t2` USING (`t1`) JOIN `t2`"},
		{"DELETE FROM t1,t2,t3 USING t1,t2,t3 where t3.a = 1", true, "DELETE FROM `t1`,`t2`,`t3` USING ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t3`.`a`=1"},
		{"DELETE FROM t2,t3 USING t1,t2,t3 where t1.a = 1", true, "DELETE FROM `t2`,`t3` USING ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t1`.`a`=1"},
		// {"DELETE FROM t2.*,t3.* USING t1,t2,t3 where t1.a = 1", true, "DELETE FROM `t2`,`t3` USING ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t1`.`a`=1"},
		// {"DELETE FROM t1,t2.*,t3.* USING t1,t2,t3 where t1.a = 1", true, "DELETE FROM `t1`,`t2`,`t3` USING ((`t1`) JOIN `t2`) JOIN `t3` WHERE `t1`.`a`=1"},

		// for delete statement
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true, "DELETE `t1`,`t2` FROM (`t1` JOIN `t2`) JOIN `t3` WHERE `t1`.`id`=`t2`.`id` AND `t2`.`id`=`t3`.`id`"},
		{"DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true, "DELETE FROM `t1`,`t2` USING (`t1` JOIN `t2`) JOIN `t3` WHERE `t1`.`id`=`t2`.`id` AND `t2`.`id`=`t3`.`id`"},
		// for optimizer hint in delete statement
		{"DELETE /*+ TiDB_INLJ(t1, t2) */ t1, t2 from t1, t2 where t1.id=t2.id;", true, "DELETE /*+ TIDB_INLJ(`t1`, `t2`)*/ `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`id`=`t2`.`id`"},
		{"DELETE /*+ TiDB_HJ(t1, t2) */ t1, t2 from t1, t2 where t1.id=t2.id", true, "DELETE /*+ TIDB_HJ(`t1`, `t2`)*/ `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`id`=`t2`.`id`"},
		{"DELETE /*+ TiDB_SMJ(t1, t2) */ t1, t2 from t1, t2 where t1.id=t2.id", true, "DELETE /*+ TIDB_SMJ(`t1`, `t2`)*/ `t1`,`t2` FROM (`t1`) JOIN `t2` WHERE `t1`.`id`=`t2`.`id`"},
		// for "USE INDEX" in delete statement
		{"DELETE FROM t1 USE INDEX(idx_a) WHERE t1.id=1;", true, "DELETE FROM `t1` USE INDEX (`idx_a`) WHERE `t1`.`id`=1"},
		{"DELETE t1, t2 FROM t1 USE INDEX(idx_a) JOIN t2 WHERE t1.id=t2.id;", true, "DELETE `t1`,`t2` FROM `t1` USE INDEX (`idx_a`) JOIN `t2` WHERE `t1`.`id`=`t2`.`id`"},
		{"DELETE t1, t2 FROM t1 USE INDEX(idx_a) JOIN t2 USE INDEX(idx_a) WHERE t1.id=t2.id;", true, "DELETE `t1`,`t2` FROM `t1` USE INDEX (`idx_a`) JOIN `t2` USE INDEX (`idx_a`) WHERE `t1`.`id`=`t2`.`id`"},

		// for fail case
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id limit 10;", false, ""},
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id order by t1.id;", false, ""},

		// for admin
		{"admin show ddl;", true, "ADMIN SHOW DDL"},
		{"admin show ddl jobs;", true, "ADMIN SHOW DDL JOBS"},
		{"admin show ddl jobs 20;", true, "ADMIN SHOW DDL JOBS 20"},
		{"admin show ddl jobs -1;", false, ""},
		{"admin show ddl job queries 1", true, "ADMIN SHOW DDL JOB QUERIES 1"},
		{"admin show ddl job queries 1, 2, 3, 4", true, "ADMIN SHOW DDL JOB QUERIES 1, 2, 3, 4"},
		// {"admin show t1 next_row_id", true, "ADMIN SHOW `t1` NEXT_ROW_ID"},
		{"admin check table t1, t2;", true, "ADMIN CHECK TABLE `t1`, `t2`"},
		{"admin check index tableName idxName;", true, "ADMIN CHECK INDEX `tableName` idxName"},
		{"admin check index tableName idxName (1, 2), (4, 5);", true, "ADMIN CHECK INDEX `tableName` idxName (1,2), (4,5)"},
		{"admin checksum table t1, t2;", true, "ADMIN CHECKSUM TABLE `t1`, `t2`"},
		{"admin cancel ddl jobs 1", true, "ADMIN CANCEL DDL JOBS 1"},
		{"admin cancel ddl jobs 1, 2", true, "ADMIN CANCEL DDL JOBS 1, 2"},
		{"admin recover index t1 idx_a", true, "ADMIN RECOVER INDEX `t1` idx_a"},
		{"admin cleanup index t1 idx_a", true, "ADMIN CLEANUP INDEX `t1` idx_a"},
		{"admin show slow top 3", true, "ADMIN SHOW SLOW TOP 3"},
		{"admin show slow top internal 7", true, "ADMIN SHOW SLOW TOP INTERNAL 7"},
		{"admin show slow top all 9", true, "ADMIN SHOW SLOW TOP ALL 9"},
		{"admin show slow recent 11", true, "ADMIN SHOW SLOW RECENT 11"},
		// {"admin reload expr_pushdown_blacklist", true, "ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST"},
		// {"admin plugins disable audit, whitelist", true, "ADMIN PLUGINS DISABLE audit, whitelist"},
		// {"admin plugins enable audit, whitelist", true, "ADMIN PLUGINS ENABLE audit, whitelist"},

		// for on duplicate key update
		{"INSERT INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);", true, "INSERT INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE `c`=VALUES(`a`)+VALUES(`b`)"},
		{"INSERT IGNORE INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);", true, "INSERT IGNORE INTO `t` (`a`,`b`,`c`) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE `c`=VALUES(`a`)+VALUES(`b`)"},

		// for insert ... set
		{"INSERT INTO t SET a=1,b=2", true, "INSERT INTO `t` SET `a`=1,`b`=2"},
		{"INSERT INTO t (a) SET a=1", false, ""},

		// for update statement
		{"UPDATE LOW_PRIORITY IGNORE t SET id = id + 1 ORDER BY id DESC;", true, "UPDATE LOW_PRIORITY IGNORE `t` SET `id`=`id`+1 ORDER BY `id` DESC"},
		{"UPDATE t SET id = id + 1 ORDER BY id DESC;", true, "UPDATE `t` SET `id`=`id`+1 ORDER BY `id` DESC"},
		{"UPDATE t SET id = id + 1 ORDER BY id DESC limit 3 ;", true, "UPDATE `t` SET `id`=`id`+1 ORDER BY `id` DESC LIMIT 3"},
		{"UPDATE t SET id = id + 1, name = 'jojo';", true, "UPDATE `t` SET `id`=`id`+1, `name`='jojo'"},
		{"UPDATE items,month SET items.price=month.price WHERE items.id=month.id;", true, "UPDATE (`items`) JOIN `month` SET `items`.`price`=`month`.`price` WHERE `items`.`id`=`month`.`id`"},
		{"UPDATE user T0 LEFT OUTER JOIN user_profile T1 ON T1.id = T0.profile_id SET T0.profile_id = 1 WHERE T0.profile_id IN (1);", true, "UPDATE `user` AS `T0` LEFT JOIN `user_profile` AS `T1` ON `T1`.`id`=`T0`.`profile_id` SET `T0`.`profile_id`=1 WHERE `T0`.`profile_id` IN (1)"},
		{"UPDATE t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, "UPDATE (`t1`) JOIN `t2` SET `t1`.`profile_id`=1, `t2`.`profile_id`=1 WHERE `ta`.`a`=`t`.`ba`"},
		// for optimizer hint in update statement
		{"UPDATE /*+ TiDB_INLJ(t1, t2) */ t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, "UPDATE /*+ TIDB_INLJ(`t1`, `t2`)*/ (`t1`) JOIN `t2` SET `t1`.`profile_id`=1, `t2`.`profile_id`=1 WHERE `ta`.`a`=`t`.`ba`"},
		{"UPDATE /*+ TiDB_SMJ(t1, t2) */ t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, "UPDATE /*+ TIDB_SMJ(`t1`, `t2`)*/ (`t1`) JOIN `t2` SET `t1`.`profile_id`=1, `t2`.`profile_id`=1 WHERE `ta`.`a`=`t`.`ba`"},
		{"UPDATE /*+ TiDB_HJ(t1, t2) */ t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, "UPDATE /*+ TIDB_HJ(`t1`, `t2`)*/ (`t1`) JOIN `t2` SET `t1`.`profile_id`=1, `t2`.`profile_id`=1 WHERE `ta`.`a`=`t`.`ba`"},
		// fail case for update statement
		{"UPDATE items,month SET items.price=month.price WHERE items.id=month.id LIMIT 10;", false, ""},
		{"UPDATE items,month SET items.price=month.price WHERE items.id=month.id order by month.id;", false, ""},
		// for "USE INDEX" in delete statement
		{"UPDATE t1 USE INDEX(idx_a) SET t1.price=3.25 WHERE t1.id=1;", true, "UPDATE `t1` USE INDEX (`idx_a`) SET `t1`.`price`=3.25 WHERE `t1`.`id`=1"},
		{"UPDATE t1 USE INDEX(idx_a) JOIN t2 SET t1.price=t2.price WHERE t1.id=t2.id;", true, "UPDATE `t1` USE INDEX (`idx_a`) JOIN `t2` SET `t1`.`price`=`t2`.`price` WHERE `t1`.`id`=`t2`.`id`"},
		{"UPDATE t1 USE INDEX(idx_a) JOIN t2 USE INDEX(idx_a) SET t1.price=t2.price WHERE t1.id=t2.id;", true, "UPDATE `t1` USE INDEX (`idx_a`) JOIN `t2` USE INDEX (`idx_a`) SET `t1`.`price`=`t2`.`price` WHERE `t1`.`id`=`t2`.`id`"},

		// for select with where clause
		{"SELECT * FROM t WHERE 1 = 1", true, "SELECT * FROM `t` WHERE 1=1"},

		// for dual
		{"select 1 from dual", true, "SELECT 1"},
		{"select 1 from dual limit 1", true, "SELECT 1 LIMIT 1"},
		{"select 1 where exists (select 2)", true, "SELECT 1 FROM DUAL WHERE EXISTS (SELECT 2)"},
		{"select 1 from dual where not exists (select 2)", true, "SELECT 1 FROM DUAL WHERE NOT EXISTS (SELECT 2)"},
		{"select 1 as a from dual order by a", true, "SELECT 1 AS `a` ORDER BY `a`"},
		{"select 1 as a from dual where 1 < any (select 2) order by a", true, "SELECT 1 AS `a` FROM DUAL WHERE 1<ANY (SELECT 2) ORDER BY `a`"},
		{"select 1 order by 1", true, "SELECT 1 ORDER BY 1"},

		//https://github.com/pingcap/tidb/issues/14297
		{"select 1 where 1=1", true, "SELECT 1 FROM DUAL WHERE 1=1"},
		//https://github.com/pingcap/tidb/issues/24496
		{"select 1 group by 1", true, "SELECT 1 GROUP BY 1"},
		{"select 1 from dual group by 1", true, "SELECT 1 GROUP BY 1"},
		// for select with partition
		{"SELECT * FROM t PARTITION (P1)", true, "SELECT * FROM `t` partition (p1)"},
		// for https://github.com/pingcap/tidb/issues/320
		{`(select 1);`, true, "(SELECT 1)"},

		// for https://github.com/pingcap/parser/issues/963
		{"select min(b) b from (select min(t.b) b from t where t.a = '');", true, "SELECT MIN(`b`) AS `b` FROM (SELECT MIN(`t`.`b`) AS `b` FROM `t` WHERE `t`.`a`=_UTF8MB4'')"},
		{"select min(b) b from (select min(t.b) b from t where t.a = '') as t1;", true, "SELECT MIN(`b`) AS `b` FROM (SELECT MIN(`t`.`b`) AS `b` FROM `t` WHERE `t`.`a`=_UTF8MB4'') AS `t1`"},
		// for mysql 8.0 Derived Tables
		// See https://dev.mysql.com/doc/refman/8.0/en/derived-tables.html
		{"select * from (select 1, 2, 3, 4) dt (a, b, c, d)", true, "SELECT * FROM (SELECT 1, 2, 3, 4) DT (`A`, `B`, `C`, `D`)"},
		{"select * from (select 1, 2, 3, 4) as dt (a, b, c, d)", true, "SELECT * FROM (SELECT 1, 2, 3, 4) AS DT (`A`, `B`, `C`, `D`)"},
		// for https://github.com/pingcap/tidb/issues/1050
		{`SELECT /*!40001 SQL_NO_CACHE */ * FROM test WHERE 1 limit 0, 2000;`, true, "SELECT SQL_NO_CACHE * FROM `test` WHERE 1 LIMIT 0,2000"},

		{`ANALYZE TABLE t`, true, "ANALYZE TABLE `t`"},

		// for comments
		{`/** 20180417 **/ show databases;`, true, "SHOW DATABASES"},
		{`/* 20180417 **/ show databases;`, true, "SHOW DATABASES"},
		{`/** 20180417 */ show databases;`, true, "SHOW DATABASES"},
		{`/** 20180417 ******/ show databases;`, true, "SHOW DATABASES"},

		// for Binlog stmt
		{`BINLOG '
BxSFVw8JAAAA8QAAAPUAAAAAAAQANS41LjQ0LU1hcmlhREItbG9nAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEzgNAAgAEgAEBAQEEgAA2QAEGggAAAAICAgCAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAA5gm5Mg==
'/*!*/;`, true, `BINLOG '
BxSFVw8JAAAA8QAAAPUAAAAAAAQANS41LjQ0LU1hcmlhREItbG9nAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEzgNAAgAEgAEBAQEEgAA2QAEGggAAAAICAgCAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAA5gm5Mg==
'`},

		// for partition table dml
		// {"select * from t1 partition (p1)", true, "SELECT * FROM `t1` PARTITION(`p1`)"},
		// {"select * from t1 partition (p1,p2)", true, "SELECT * FROM `t1` PARTITION(`p1`, `p2`)"},
		// {"select * from t1 partition (`p1`, p2, p3)", true, "SELECT * FROM `t1` PARTITION(`p1`, `p2`, `p3`)"},
		// {`select * from t1 partition ()`, false, ""},

		// for split table index region syntax
		// {"split table t1 index idx1 by ('a'),('b'),('c')", true, "SPLIT TABLE `t1` INDEX `idx1` BY ('a'),('b'),('c')"},
		// {"split table t1 index idx1 by (1)", true, "SPLIT TABLE `t1` INDEX `idx1` BY (1)"},
		// {"split table t1 index idx1 by ('abc',123), ('xyz'), ('yz', 1000)", true, "SPLIT TABLE `t1` INDEX `idx1` BY ('abc',123),('xyz'),('yz',1000)"},
		// {"split table t1 index idx1 by ", false, ""},
		// {"split table t1 index idx1 between ('a') and ('z') regions 10", true, "SPLIT TABLE `t1` INDEX `idx1` BETWEEN ('a') AND ('z') REGIONS 10"},
		// {"split table t1 index idx1 between ('a',1) and ('z',2) regions 10", true, "SPLIT TABLE `t1` INDEX `idx1` BETWEEN ('a',1) AND ('z',2) REGIONS 10"},
		// {"split table t1 index idx1 between () and () regions 10", true, "SPLIT TABLE `t1` INDEX `idx1` BETWEEN () AND () REGIONS 10"},
		// {"split table t1 index by (1)", false, ""},

		// for split table region.
		// {"split table t1 by ('a'),('b'),('c')", true, "SPLIT TABLE `t1` BY ('a'),('b'),('c')"},
		// {"split table t1 by (1)", true, "SPLIT TABLE `t1` BY (1)"},
		// {"split table t1 by ('abc',123), ('xyz'), ('yz', 1000)", true, "SPLIT TABLE `t1` BY ('abc',123),('xyz'),('yz',1000)"},
		// {"split table t1 by ", false, ""},
		// {"split table t1 between ('a') and ('z') regions 10", true, "SPLIT TABLE `t1` BETWEEN ('a') AND ('z') REGIONS 10"},
		// {"split table t1 between ('a',1) and ('z',2) regions 10", true, "SPLIT TABLE `t1` BETWEEN ('a',1) AND ('z',2) REGIONS 10"},
		// {"split table t1 between () and () regions 10", true, "SPLIT TABLE `t1` BETWEEN () AND () REGIONS 10"},

		// for show table regions.
		// {"show table t1 regions", true, "SHOW TABLE `t1` REGIONS"},
		// {"show table t1 regions where a=1", true, "SHOW TABLE `t1` REGIONS WHERE `a`=1"},
		{"show table t1", false, ""},
		// {"show table t1 index idx1 regions", true, "SHOW TABLE `t1` INDEX `idx1` REGIONS"},
		// {"show table t1 index idx1 regions where a=2", true, "SHOW TABLE `t1` INDEX `idx1` REGIONS WHERE `a`=2"},
		// {"show table t1 index idx1", false, ""},

		// for transaction mode
		// {"begin pessimistic", true, "BEGIN PESSIMISTIC"},
		// {"begin optimistic", true, "BEGIN OPTIMISTIC"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestDBAStmt(c *C) {
	table := []testCase{
		// for SHOW statement
		{"SHOW VARIABLES LIKE 'character_set_results'", true, "SHOW SESSION VARIABLES LIKE 'character_set_results'"},
		{"SHOW GLOBAL VARIABLES LIKE 'character_set_results'", true, "SHOW GLOBAL VARIABLES LIKE 'character_set_results'"},
		{"SHOW SESSION VARIABLES LIKE 'character_set_results'", true, "SHOW SESSION VARIABLES LIKE 'character_set_results'"},
		{"SHOW VARIABLES", true, "SHOW SESSION VARIABLES"},
		{"SHOW GLOBAL VARIABLES", true, "SHOW GLOBAL VARIABLES"},
		{"SHOW GLOBAL VARIABLES WHERE Variable_name = 'autocommit'", true, "SHOW GLOBAL VARIABLES WHERE `Variable_name`='autocommit'"},
		{"SHOW STATUS", true, "SHOW SESSION STATUS"},
		{"SHOW GLOBAL STATUS", true, "SHOW GLOBAL STATUS"},
		{"SHOW SESSION STATUS", true, "SHOW SESSION STATUS"},
		{`SHOW STATUS LIKE 'Up%'`, true, "SHOW SESSION STATUS LIKE 'Up%'"},
		{`SHOW STATUS WHERE Variable_name`, true, "SHOW SESSION STATUS WHERE `Variable_name`"},
		{`SHOW STATUS WHERE Variable_name LIKE 'Up%'`, true, "SHOW SESSION STATUS WHERE `Variable_name` LIKE 'Up%'"},
		{`SHOW FULL TABLES FROM icar_qa LIKE play_evolutions`, true, "SHOW FULL TABLES IN `icar_qa` LIKE `play_evolutions`"},
		{`SHOW FULL TABLES WHERE Table_Type != 'VIEW'`, true, "SHOW FULL TABLES WHERE `Table_Type`!='VIEW'"},
		{`SHOW GRANTS`, true, "SHOW GRANTS"},
		{`SHOW GRANTS FOR 'test'@'localhost'`, true, "SHOW GRANTS FOR `test`@`localhost`"},
		{`SHOW GRANTS FOR current_user()`, true, "SHOW GRANTS FOR CURRENT_USER"},
		{`SHOW GRANTS FOR current_user`, true, "SHOW GRANTS FOR CURRENT_USER"},
		// {`SHOW GRANTS FOR 'u1'@'localhost' USING 'r1'`, true, "SHOW GRANTS FOR `u1`@`localhost` USING `r1`@`%`"},
		// {`SHOW GRANTS FOR 'u1'@'localhost' USING 'r1', 'r2'`, true, "SHOW GRANTS FOR `u1`@`localhost` USING `r1`@`%`, `r2`@`%`"},
		{`SHOW COLUMNS FROM City;`, true, "SHOW COLUMNS IN `City`"},
		{`SHOW COLUMNS FROM tv189.1_t_1_x;`, true, "SHOW COLUMNS IN `tv189`.`1_t_1_x`"},
		{`SHOW FIELDS FROM City;`, true, "SHOW COLUMNS IN `City`"},
		{`SHOW TRIGGERS LIKE 't'`, true, "SHOW TRIGGERS LIKE 't'"},
		{`SHOW DATABASES LIKE 'test2'`, true, "SHOW DATABASES LIKE 'test2'"},
		// PROCEDURE and FUNCTION are currently not supported.
		// And FUNCTION reuse show procedure status process logic.
		{`SHOW PROCEDURE STATUS WHERE Db='test'`, true, "SHOW PROCEDURE STATUS WHERE `Db`='test'"},
		{`SHOW FUNCTION STATUS WHERE Db='test'`, true, "SHOW PROCEDURE STATUS WHERE `Db`='test'"},
		{`SHOW INDEX FROM t;`, true, "SHOW INDEX IN `t`"},
		{`SHOW KEYS FROM t;`, true, "SHOW INDEX IN `t`"},
		{`SHOW INDEX IN t;`, true, "SHOW INDEX IN `t`"},
		{`SHOW KEYS IN t;`, true, "SHOW INDEX IN `t`"},
		{`SHOW INDEXES IN t where true;`, true, "SHOW INDEX IN `t` WHERE TRUE"},
		{`SHOW KEYS FROM t FROM test where true;`, true, "SHOW INDEX IN `test`.`t` WHERE TRUE"},
		{`SHOW EVENTS FROM test_db WHERE definer = 'current_user'`, true, "SHOW EVENTS IN `test_db` WHERE `definer`='current_user'"},
		{`SHOW PLUGINS`, true, "SHOW PLUGINS"},
		// {`SHOW PROFILES`, true, "SHOW PROFILES"},
		// {`SHOW PROFILE`, true, "SHOW PROFILE"},
		// {`SHOW PROFILE FOR QUERY 1`, true, "SHOW PROFILE FOR QUERY 1"},
		// {`SHOW PROFILE CPU FOR QUERY 2`, true, "SHOW PROFILE CPU FOR QUERY 2"},
		// {`SHOW PROFILE CPU FOR QUERY 2 LIMIT 1,1`, true, "SHOW PROFILE CPU FOR QUERY 2 LIMIT 1,1"},
		// {`SHOW PROFILE CPU, MEMORY, BLOCK IO, CONTEXT SWITCHES, PAGE FAULTS, IPC, SWAPS, SOURCE FOR QUERY 1 limit 100`, true, "SHOW PROFILE CPU, MEMORY, BLOCK IO, CONTEXT SWITCHES, PAGE FAULTS, IPC, SWAPS, SOURCE FOR QUERY 1 LIMIT 100"},
		{`SHOW MASTER STATUS`, true, "SHOW MASTER STATUS"},
		{`SHOW PRIVILEGES`, true, "SHOW PRIVILEGES"},
		// for show character set
		{"show character set;", true, "SHOW CHARSET"},
		{"show charset", true, "SHOW CHARSET"},
		// for show collation
		{"show collation", true, "SHOW COLLATION"},
		{`show collation like 'utf8%'`, true, "SHOW COLLATION LIKE 'utf8%'"},
		{"show collation where Charset = 'utf8' and Collation = 'utf8_bin'", true, "SHOW COLLATION WHERE `Charset`='utf8' AND `Collation`='utf8_bin'"},
		// for show full columns
		{"show columns in t;", true, "SHOW COLUMNS IN `t`"},
		{"show full columns in t;", true, "SHOW FULL COLUMNS IN `t`"},
		// for show create table
		{"show create table test.t", true, "SHOW CREATE TABLE `test`.`t`"},
		{"show create table t", true, "SHOW CREATE TABLE `t`"},
		// for show create view
		// {"show create view test.t", true, "SHOW CREATE VIEW `test`.`t`"},
		// {"show create view t", true, "SHOW CREATE VIEW `t`"},
		// for show create database
		{"show create database d1", true, "SHOW CREATE DATABASE `d1`"},
		// {"show create database if not exists d1", true, "SHOW CREATE DATABASE IF NOT EXISTS `d1`"},
		// for show stats_meta.
		{"show stats_meta", true, "SHOW STATS_META"},
		{"show stats_meta where table_name = 't'", true, "SHOW STATS_META WHERE `table_name`='t'"},
		// for show stats_histograms
		{"show stats_histograms", true, "SHOW STATS_HISTOGRAMS"},
		{"show stats_histograms where col_name = 'a'", true, "SHOW STATS_HISTOGRAMS WHERE `col_name`='a'"},
		// for show stats_buckets
		{"show stats_buckets", true, "SHOW STATS_BUCKETS"},
		{"show stats_buckets where col_name = 'a'", true, "SHOW STATS_BUCKETS WHERE `col_name`='a'"},
		// for show stats_healthy.
		{"show stats_healthy", true, "SHOW STATS_HEALTHY"},
		{"show stats_healthy where table_name = 't'", true, "SHOW STATS_HEALTHY WHERE `table_name`='t'"},
		// for show pump/drainer status.
		// {"show pump status", true, "SHOW PUMP STATUS"},
		// {"show drainer status", true, "SHOW DRAINER STATUS"},
		// {"show analyze status", true, "SHOW ANALYZE STATUS"},
		// {"show analyze status where table_name = 't'", true, "SHOW ANALYZE STATUS WHERE `table_name`='t'"},
		// {"show analyze status where table_name like '%'", true, "SHOW ANALYZE STATUS WHERE `table_name` LIKE '%'"},

		// for load stats
		// {"load stats '/tmp/stats.json'", true, "LOAD STATS '/tmp/stats.json'"},
		// set
		// user defined
		{"SET @ = 1", true, "SET @``=1"},
		{"SET @' ' = 1", true, "SET @` `=1"},
		{"SET @! = 1", false, ""},
		{"SET @1 = 1", true, "SET @`1`=1"},
		{"SET @a = 1", true, "SET @`a`=1"},
		{"SET @b := 1", true, "SET @`b`=1"},
		{"SET @.c = 1", true, "SET @`.c`=1"},
		{"SET @_d = 1", true, "SET @`_d`=1"},
		{"SET @_e._$. = 1", true, "SET @`_e._$.`=1"},
		{"SET @~f = 1", false, ""},
		{"SET @`g,` = 1", true, "SET @`g,`=1"},
		// session system variables
		{"SET SESSION autocommit = 1", true, "SET @@SESSION.`autocommit`=1"},
		{"SET @@session.autocommit = 1", true, "SET @@SESSION.`autocommit`=1"},
		{"SET @@SESSION.autocommit = 1", true, "SET @@SESSION.`autocommit`=1"},
		{"SET @@GLOBAL.GTID_PURGED = '123'", true, "SET @@GLOBAL.`gtid_purged`='123'"},
		{"SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN", true, "SET @`MYSQLDUMP_TEMP_LOG_BIN`=@@SESSION.`sql_log_bin`"},
		{"SET LOCAL autocommit = 1", true, "SET @@SESSION.`autocommit`=1"},
		{"SET @@local.autocommit = 1", true, "SET @@SESSION.`autocommit`=1"},
		{"SET @@autocommit = 1", true, "SET @@SESSION.`autocommit`=1"},
		{"SET autocommit = 1", true, "SET @@SESSION.`autocommit`=1"},
		// global system variables
		{"SET GLOBAL autocommit = 1", true, "SET @@GLOBAL.`autocommit`=1"},
		{"SET @@global.autocommit = 1", true, "SET @@GLOBAL.`autocommit`=1"},
		// set through mysql extension assignment syntax
		// {"SET autocommit := 1", true, "SET @@SESSION.`autocommit`=1"},
		// {"SET @@session.autocommit := 1", true, "SET @@SESSION.`autocommit`=1"},
		// {"SET @MYSQLDUMP_TEMP_LOG_BIN := @@SESSION.SQL_LOG_BIN", true, "SET @`MYSQLDUMP_TEMP_LOG_BIN`=@@SESSION.`sql_log_bin`"},
		// {"SET LOCAL autocommit := 1", true, "SET @@SESSION.`autocommit`=1"},
		// {"SET @@global.autocommit := default", true, "SET @@GLOBAL.`autocommit`=DEFAULT"},
		// // set default value
		// {"SET @@global.autocommit = default", true, "SET @@GLOBAL.`autocommit`=DEFAULT"},
		// {"SET @@session.autocommit = default", true, "SET @@SESSION.`autocommit`=DEFAULT"},
		// SET CHARACTER SET
		{"SET CHARACTER SET utf8mb4;", true, "SET NAMES 'utf8mb4'"},
		{"SET CHARACTER SET 'utf8mb4';", true, "SET NAMES 'utf8mb4'"},
		// set password
		{"SET PASSWORD = 'password';", true, "SET PASSWORD='password'"},
		{"SET PASSWORD FOR 'root'@'localhost' = 'password';", true, "SET PASSWORD FOR `root`@`localhost`='password'"},
		// SET TRANSACTION Syntax
		{"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", true, "SET @@SESSION.`tx_isolation`='REPEATABLE-READ'"},
		{"SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ", true, "SET @@GLOBAL.`tx_isolation`='REPEATABLE-READ'"},
		{"SET SESSION TRANSACTION READ WRITE", true, "SET @@SESSION.`tx_read_only`='0'"},
		{"SET SESSION TRANSACTION READ ONLY", true, "SET @@SESSION.`tx_read_only`='1'"},
		{"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", true, "SET @@SESSION.`tx_isolation`='READ-COMMITTED'"},
		{"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", true, "SET @@SESSION.`tx_isolation`='READ-UNCOMMITTED'"},
		{"SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", true, "SET @@SESSION.`tx_isolation`='SERIALIZABLE'"},
		{"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", true, "SET @@SESSION.`tx_isolation_one_shot`='REPEATABLE-READ'"},
		{"SET TRANSACTION READ WRITE", true, "SET @@SESSION.`tx_read_only`='0'"},
		{"SET TRANSACTION READ ONLY", true, "SET @@SESSION.`tx_read_only`='1'"},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", true, "SET @@SESSION.`tx_isolation_one_shot`='READ-COMMITTED'"},
		{"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", true, "SET @@SESSION.`tx_isolation_one_shot`='READ-UNCOMMITTED'"},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", true, "SET @@SESSION.`tx_isolation_one_shot`='SERIALIZABLE'"},
		// for set names
		{"set names utf8", true, "SET NAMES 'utf8'"},
		{"set names utf8 collate utf8_unicode_ci", true, "SET NAMES 'utf8' COLLATE 'utf8_unicode_ci'"},
		{"set names binary", true, "SET NAMES 'binary'"},
		// {"set role `role1`", true, "SET ROLE `role1`@`%`"},
		// {"SET ROLE DEFAULT", true, "SET ROLE DEFAULT"},
		// {"SET ROLE ALL", true, "SET ROLE ALL"},
		// {"SET ROLE ALL EXCEPT `role1`, `role2`", true, "SET ROLE ALL EXCEPT `role1`@`%`, `role2`@`%`"},
		// {"SET DEFAULT ROLE administrator, developer TO `joe`@`10.0.0.1`", true, "SET DEFAULT ROLE `administrator`@`%`, `developer`@`%` TO `joe`@`10.0.0.1`"},
		// for set names and set vars
		{"set names utf8, @@session.sql_mode=1;", true, "SET NAMES 'utf8', @@SESSION.`sql_mode`=1"},
		{"set @@session.sql_mode=1, names utf8, charset utf8;", true, "SET @@SESSION.`sql_mode`=1, NAMES 'utf8', NAMES 'utf8'"},

		// for FLUSH statement
		{"flush no_write_to_binlog tables tbl1 with read lock", true, "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1` WITH READ LOCK"},
		{"flush table", true, "FLUSH TABLES"},
		{"flush tables", true, "FLUSH TABLES"},
		{"flush tables tbl1", true, "FLUSH TABLES `tbl1`"},
		{"flush no_write_to_binlog tables tbl1", true, "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1`"},
		{"flush local tables tbl1", true, "FLUSH NO_WRITE_TO_BINLOG TABLES `tbl1`"},
		{"flush table with read lock", true, "FLUSH TABLES WITH READ LOCK"},
		{"flush tables tbl1, tbl2, tbl3", true, "FLUSH TABLES `tbl1`, `tbl2`, `tbl3`"},
		{"flush tables tbl1, tbl2, tbl3 with read lock", true, "FLUSH TABLES `tbl1`, `tbl2`, `tbl3` WITH READ LOCK"},
		{"flush privileges", true, "FLUSH PRIVILEGES"},
		{"flush status", true, "FLUSH STATUS"},
		// {"flush tidb plugins plugin1", true, "FLUSH TIDB PLUGINS plugin1"},
		// {"flush tidb plugins plugin1, plugin2", true, "FLUSH TIDB PLUGINS plugin1, plugin2"},

		// for change statement
		// {"change pump to node_state ='paused' for node_id '127.0.0.1:8250'", true, "CHANGE PUMP TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:8250'"},
		// {"change drainer to node_state ='paused' for node_id '127.0.0.1:8249'", true, "CHANGE DRAINER TO NODE_STATE ='paused' FOR NODE_ID '127.0.0.1:8249'"},
		// for call statement
		{"call ", false, ""},
		{"call test", true, "CALL `test`()"},
		{"call test()", true, "CALL `test`()"},
		{"call test(1, 'test', true)", true, "CALL `test`(1, _UTF8MB4'test', TRUE)"},
		{"call x.y;", true, "CALL `x`.`y`()"},
		{"call x.y();", true, "CALL `x`.`y`()"},
		{"call x.y('p', 'q', 'r');", true, "CALL `x`.`y`(_UTF8MB4'p', _UTF8MB4'q', _UTF8MB4'r')"},
		{"call `x`.`y`;", true, "CALL `x`.`y`()"},
		{"call `x`.`y`();", true, "CALL `x`.`y`()"},
		{"call `x`.`y`('p', 'q', 'r');", true, "CALL `x`.`y`(_UTF8MB4'p', _UTF8MB4'q', _UTF8MB4'r')"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestFlushTable(c *C) {
	parser := New()
	stmt, _, err := parser.Parse("flush local tables tbl1,tbl2 with read lock", "", "")
	c.Assert(err, IsNil)
	flushTable := stmt[0].(*ast.FlushStmt)
	c.Assert(flushTable.Tp, Equals, ast.FlushTables)
	c.Assert(flushTable.Tables[0].Name.L, Equals, "tbl1")
	c.Assert(flushTable.Tables[1].Name.L, Equals, "tbl2")
	c.Assert(flushTable.NoWriteToBinLog, IsTrue)
	c.Assert(flushTable.ReadLock, IsTrue)
}

func (s *testParserSuite) TestFlushPrivileges(c *C) {
	parser := New()
	stmt, _, err := parser.Parse("flush privileges", "", "")
	c.Assert(err, IsNil)
	flushPrivilege := stmt[0].(*ast.FlushStmt)
	c.Assert(flushPrivilege.Tp, Equals, ast.FlushPrivileges)
}

func (s *testParserSuite) TestExpression(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// sign expression
		{"SELECT ++1", true, "SELECT ++1"},
		{"SELECT -*1", false, "SELECT -*1"},
		{"SELECT -+1", true, "SELECT -+1"},
		{"SELECT -1", true, "SELECT -1"},
		{"SELECT --1", true, "SELECT --1"},

		// for string literal
		{`select '''a''', """a"""`, true, "SELECT '''a''','\"a\"'"},
		{`select ''a''`, false, ""},
		{`select ""a""`, false, ""},
		{`select '''a''';`, true, "SELECT '''a'''"},
		{`select '\'a\'';`, true, "SELECT '''a'''"},
		{`select "\"a\"";`, true, "SELECT '\"a\"'"},
		{`select """a""";`, true, "SELECT '\"a\"'"},
		{`select _utf8"string";`, true, "SELECT _UTF8'string'"},
		{`select _binary"string";`, true, "SELECT _BINARY'string'"},
		{"select N'string'", true, "SELECT _UTF8'string'"},
		{"select n'string'", true, "SELECT _UTF8'string'"},
		// for comparison
		{"select 1 <=> 0, 1 <=> null, 1 = null", true, "SELECT 1<=>0,1<=>NULL,1=NULL"},
		// for date literal
		{"select date'1989-09-10'", true, "SELECT DATELITERAL('1989-09-10')"},
		{"select date 19890910", false, ""},
		// for time literal
		{"select time '00:00:00.111'", true, "SELECT TIMELITERAL('00:00:00.111')"},
		{"select time 19890910", false, ""},
		// for timestamp literal
		{"select timestamp '1989-09-10 11:11:11'", true, "SELECT TIMESTAMPLITERAL('1989-09-10 11:11:11')"},
		{"select timestamp 19890910", false, ""},

		// The ODBC syntax for time/date/timestamp literal.
		// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
		{"select {ts '1989-09-10 11:11:11'}", true, "SELECT TIMESTAMP '1989-09-10 11:11:11'"},
		{"select {d '1989-09-10'}", true, "SELECT DATE '1989-09-10'"},
		{"select {t '00:00:00.111'}", true, "SELECT TIME '00:00:00.111'"},
		{"select * from t where a > {ts '1989-09-10 11:11:11'}", true, "SELECT * FROM `t` WHERE `a`>TIMESTAMP '1989-09-10 11:11:11'"},
		{"select * from t where a > {ts {abc '1989-09-10 11:11:11'}}", true, "SELECT * FROM `t` WHERE `a`>TIMESTAMP '1989-09-10 11:11:11'"},
		// If the identifier is not in (t, d, ts), we just ignore it and consider the following expression as the value.
		// See: https://dev.mysql.com/doc/refman/5.7/en/expressions.html
		{"select {ts123 '1989-09-10 11:11:11'}", true, "SELECT '1989-09-10 11:11:11'"},
		{"select {ts123 123}", true, "SELECT 123"},
		{"select {ts123 1 xor 1}", true, "SELECT 1 XOR 1"},
		{"select * from t where a > {ts123 '1989-09-10 11:11:11'}", true, "SELECT * FROM `t` WHERE `a`>_UTF8MB4'1989-09-10 11:11:11'"},
		{"select .t.a from t", false, ""},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestBuiltin(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// for builtin functions
		{"SELECT POW(1, 2)", true, "SELECT POW(1, 2)"},
		{"SELECT POW(1, 2, 1)", true, "SELECT POW(1, 2, 1)"}, // illegal number of arguments shall pass too
		{"SELECT POW(1, 0.5)", true, "SELECT POW(1, 0.5)"},
		{"SELECT POW(1, -1)", true, "SELECT POW(1, -1)"},
		{"SELECT POW(-1, 1)", true, "SELECT POW(-1, 1)"},
		{"SELECT RAND();", true, "SELECT RAND()"},
		{"SELECT RAND(1);", true, "SELECT RAND(1)"},
		{"SELECT MOD(10, 2);", true, "SELECT 10%2"},
		{"SELECT ROUND(-1.23);", true, "SELECT ROUND(-1.23)"},
		{"SELECT ROUND(1.23, 1);", true, "SELECT ROUND(1.23, 1)"},
		{"SELECT ROUND(1.23, 1, 1);", true, "SELECT ROUND(1.23, 1, 1)"},
		{"SELECT CEIL(-1.23);", true, "SELECT CEIL(-1.23)"},
		{"SELECT CEILING(1.23);", true, "SELECT CEILING(1.23)"},
		{"SELECT FLOOR(-1.23);", true, "SELECT FLOOR(-1.23)"},
		{"SELECT LN(1);", true, "SELECT LN(1)"},
		{"SELECT LN(1, 2);", true, "SELECT LN(1, 2)"},
		{"SELECT LOG(-2);", true, "SELECT LOG(-2)"},
		{"SELECT LOG(2, 65536);", true, "SELECT LOG(2, 65536)"},
		{"SELECT LOG(2, 65536, 1);", true, "SELECT LOG(2, 65536, 1)"},
		{"SELECT LOG2(2);", true, "SELECT LOG2(2)"},
		{"SELECT LOG2(2, 2);", true, "SELECT LOG2(2, 2)"},
		{"SELECT LOG10(10);", true, "SELECT LOG10(10)"},
		{"SELECT LOG10(10, 1);", true, "SELECT LOG10(10, 1)"},
		{"SELECT ABS(10, 1);", true, "SELECT ABS(10, 1)"},
		{"SELECT ABS(10);", true, "SELECT ABS(10)"},
		{"SELECT ABS();", true, "SELECT ABS()"},
		{"SELECT CONV(10+'10'+'10'+X'0a',10,10);", true, "SELECT CONV(10+'10'+'10'+x'0a', 10, 10)"},
		{"SELECT CONV();", true, "SELECT CONV()"},
		{"SELECT CRC32('MySQL');", true, "SELECT CRC32('MySQL')"},
		{"SELECT CRC32();", true, "SELECT CRC32()"},
		{"SELECT SIGN();", true, "SELECT SIGN()"},
		{"SELECT SIGN(0);", true, "SELECT SIGN(0)"},
		{"SELECT SQRT(0);", true, "SELECT SQRT(0)"},
		{"SELECT SQRT();", true, "SELECT SQRT()"},
		{"SELECT ACOS();", true, "SELECT ACOS()"},
		{"SELECT ACOS(1);", true, "SELECT ACOS(1)"},
		{"SELECT ACOS(1, 2);", true, "SELECT ACOS(1, 2)"},
		{"SELECT ASIN();", true, "SELECT ASIN()"},
		{"SELECT ASIN(1);", true, "SELECT ASIN(1)"},
		{"SELECT ASIN(1, 2);", true, "SELECT ASIN(1, 2)"},
		{"SELECT ATAN(0), ATAN(1), ATAN(1, 2);", true, "SELECT ATAN(0),ATAN(1),ATAN(1, 2)"},
		{"SELECT ATAN2(), ATAN2(1,2);", true, "SELECT ATAN2(),ATAN2(1, 2)"},
		{"SELECT COS(0);", true, "SELECT COS(0)"},
		{"SELECT COS(1);", true, "SELECT COS(1)"},
		{"SELECT COS(1, 2);", true, "SELECT COS(1, 2)"},
		{"SELECT COT();", true, "SELECT COT()"},
		{"SELECT COT(1);", true, "SELECT COT(1)"},
		{"SELECT COT(1, 2);", true, "SELECT COT(1, 2)"},
		{"SELECT DEGREES();", true, "SELECT DEGREES()"},
		{"SELECT DEGREES(0);", true, "SELECT DEGREES(0)"},
		{"SELECT EXP();", true, "SELECT EXP()"},
		{"SELECT EXP(1);", true, "SELECT EXP(1)"},
		{"SELECT PI();", true, "SELECT PI()"},
		{"SELECT PI(1);", true, "SELECT PI(1)"},
		{"SELECT RADIANS();", true, "SELECT RADIANS()"},
		{"SELECT RADIANS(1);", true, "SELECT RADIANS(1)"},
		{"SELECT SIN();", true, "SELECT SIN()"},
		{"SELECT SIN(1);", true, "SELECT SIN(1)"},
		{"SELECT TAN(1);", true, "SELECT TAN(1)"},
		{"SELECT TAN();", true, "SELECT TAN()"},
		{"SELECT TRUNCATE(1.223,1);", true, "SELECT TRUNCATE(1.223, 1)"},
		{"SELECT TRUNCATE();", true, "SELECT TRUNCATE()"},

		{"SELECT SUBSTR('Quadratically',5);", true, "SELECT SUBSTR('Quadratically', 5)"},
		{"SELECT SUBSTR('Quadratically',5, 3);", true, "SELECT SUBSTR('Quadratically', 5, 3)"},
		{"SELECT SUBSTR('Quadratically' FROM 5);", true, "SELECT SUBSTR('Quadratically', 5)"},
		{"SELECT SUBSTR('Quadratically' FROM 5 FOR 3);", true, "SELECT SUBSTR('Quadratically', 5, 3)"},

		{"SELECT SUBSTRING('Quadratically',5);", true, "SELECT SUBSTRING('Quadratically', 5)"},
		{"SELECT SUBSTRING('Quadratically',5, 3);", true, "SELECT SUBSTRING('Quadratically', 5, 3)"},
		{"SELECT SUBSTRING('Quadratically' FROM 5);", true, "SELECT SUBSTRING('Quadratically', 5)"},
		{"SELECT SUBSTRING('Quadratically' FROM 5 FOR 3);", true, "SELECT SUBSTRING('Quadratically', 5, 3)"},

		{"SELECT CONVERT('111', SIGNED);", true, "SELECT CONVERT('111', SIGNED)"},

		{"SELECT LEAST(), LEAST(1, 2, 3);", true, "SELECT LEAST(),LEAST(1, 2, 3)"},

		{"SELECT INTERVAL(1, 0, 1, 2)", true, "SELECT INTERVAL(1, 0, 1, 2)"},
		{"SELECT (INTERVAL(1, 0, 1, 2)+5)*7+INTERVAL(1, 0, 1, 2)/2", true, "SELECT (INTERVAL(1, 0, 1, 2)+5)*7+INTERVAL(1, 0, 1, 2)/2"},
		{"SELECT INTERVAL(0, (1*5)/2)+INTERVAL(5, 4, 3)", true, "SELECT INTERVAL(0, (1*5)/2)+INTERVAL(5, 4, 3)"},
		{"SELECT DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY);", true, "SELECT DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY)"},

		// information functions
		{"SELECT DATABASE();", true, "SELECT DATABASE()"},
		{"SELECT SCHEMA();", true, "SELECT SCHEMA()"},
		{"SELECT USER();", true, "SELECT USER()"},
		{"SELECT USER(1);", true, "SELECT USER(1)"},
		{"SELECT CURRENT_USER();", true, "SELECT CURRENT_USER()"},
		{"SELECT CURRENT_ROLE();", true, "SELECT CURRENT_ROLE()"},
		{"SELECT CURRENT_USER;", true, "SELECT CURRENT_USER()"},
		{"SELECT CONNECTION_ID();", true, "SELECT CONNECTION_ID()"},
		{"SELECT VERSION();", true, "SELECT VERSION()"},
		{"SELECT BENCHMARK(1000000, AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3')));", true, "SELECT BENCHMARK(1000000, AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3')))"},
		{"SELECT BENCHMARK(AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3')));", true, "SELECT BENCHMARK(AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3')))"},
		{"SELECT CHARSET('abc');", true, "SELECT CHARSET('abc')"},
		{"SELECT COERCIBILITY('abc');", true, "SELECT COERCIBILITY('abc')"},
		{"SELECT COERCIBILITY('abc', 'a');", true, "SELECT COERCIBILITY('abc', 'a')"},
		{"SELECT COLLATION('abc');", true, "SELECT COLLATION('abc')"},
		{"SELECT ROW_COUNT();", true, "SELECT ROW_COUNT()"},
		{"SELECT SESSION_USER();", true, "SELECT SESSION_USER()"},
		{"SELECT SYSTEM_USER();", true, "SELECT SYSTEM_USER()"},

		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);", true, "SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2)"},
		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);", true, "SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2)"},

		{`SELECT ASCII(), ASCII(""), ASCII("A"), ASCII(1);`, true, "SELECT ASCII(),ASCII(''),ASCII('A'),ASCII(1)"},

		{`SELECT LOWER("A"), UPPER("a")`, true, "SELECT LOWER('A'),UPPER('a')"},
		{`SELECT LCASE("A"), UCASE("a")`, true, "SELECT LCASE('A'),UCASE('a')"},

		{`SELECT REPLACE('www.mysql.com', 'w', 'Ww')`, true, "SELECT REPLACE('www.mysql.com', 'w', 'Ww')"},

		{`SELECT LOCATE('bar', 'foobarbar');`, true, "SELECT LOCATE('bar', 'foobarbar')"},
		{`SELECT LOCATE('bar', 'foobarbar', 5);`, true, "SELECT LOCATE('bar', 'foobarbar', 5)"},

		{`SELECT tidb_version();`, true, "SELECT TIDB_VERSION()"},
		{`SELECT tidb_is_ddl_owner();`, true, "SELECT TIDB_IS_DDL_OWNER()"},

		{`SELECT tidb_decode_plan();`, true, "SELECT TIDB_DECODE_PLAN()"},

		// for time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true, "CREATE TABLE `t` (`c1` TIME(2),`c2` DATETIME(2),`c3` TIMESTAMP(2))"},

		// for row
		{"select row(1)", false, ""},
		{"select row(1, 1,)", false, ""},
		{"select (1, 1,)", false, ""},
		{"select row(1, 1) > row(1, 1), row(1, 1, 1) > row(1, 1, 1)", true, "SELECT ROW(1,1)>ROW(1,1),ROW(1,1,1)>ROW(1,1,1)"},
		{"Select (1, 1) > (1, 1)", true, "SELECT ROW(1,1)>ROW(1,1)"},
		{"create table t (`row` int)", true, "CREATE TABLE `t` (`row` INT)"},
		// {"create table t (row int)", false, ""},

		// for cast with charset
		{"SELECT *, CAST(data AS CHAR CHARACTER SET utf8) FROM t;", true, "SELECT *,CAST(`data` AS CHAR CHARSET UTF8) FROM `t`"},
		{"SELECT CAST(data AS CHARACTER);", true, "SELECT CAST(`data` AS CHAR)"},
		{"SELECT CAST(data AS CHARACTER(10) CHARACTER SET utf8);", true, "SELECT CAST(`data` AS CHAR(10) CHARSET UTF8)"},
		{"SELECT CAST(data AS BINARY)", true, "SELECT CAST(`data` AS BINARY)"},
		// for cast as JSON
		{"SELECT *, CAST(data AS JSON) FROM t;", true, "SELECT *,CAST(`data` AS JSON) FROM `t`"},
		// for cast as number
		{"SELECT CAST(data AS NUMBER(10,2)) FROM t;", true, "SELECT CAST(`data` AS NUMBER(10,2)) FROM `t`"},
		//
		{`SELECT 1 member of (a)`, true, "SELECT 1 MEMBER OF (`a`)"},
		{`SELECT 1 member of a`, false, ""},
		{`SELECT 1 member a`, false, ""},
		{`SELECT 1 not member of a`, false, ""},
		{`SELECT 1 member of (1+1)`, false, ""},
		{`SELECT concat('a') member of (cast(1 as char(1)))`, true, "SELECT CONCAT(_UTF8MB4'a') MEMBER OF (CAST(1 AS CHAR(1)))"},
		// for cast as signed int, fix issue #3691.
		{"select cast(1 as signed int);", true, "SELECT CAST(1 AS SIGNED)"},

		// for cast as double
		{"select cast(1 as double);", true, "SELECT CAST(1 AS DOUBLE)"},

		// for cast as float
		{"select cast(1 as float);", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(0));", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(24));", true, "SELECT CAST(1 AS FLOAT)"},
		{"select cast(1 as float(25));", true, "SELECT CAST(1 AS DOUBLE)"},
		{"select cast(1 as float(53));", true, "SELECT CAST(1 AS DOUBLE)"},
		{"select cast(1 as float(54));", false, ""},

		// for cast as real
		{"select cast(1 as real);", true, "SELECT CAST(1 AS DOUBLE)"},
		{"select cast('2000' as year);", true, "SELECT CAST(_UTF8MB4'2000' AS YEAR)"},
		{"select cast(time '2000' as year);", true, "SELECT CAST(TIME '2000' AS YEAR)"},

		{"select cast(b as signed array);", true, "SELECT CAST(`b` AS SIGNED ARRAY)"},
		{"select cast(b as char(10) array);", true, "SELECT CAST(`b` AS CHAR(10) ARRAY)"},

		// for last_insert_id
		{"SELECT last_insert_id();", true, "SELECT LAST_INSERT_ID()"},
		{"SELECT last_insert_id(1);", true, "SELECT LAST_INSERT_ID(1)"},

		// for binary operator
		{"SELECT binary 'a';", true, "SELECT BINARY 'a'"},

		// for bit_count
		{`SELECT BIT_COUNT(1);`, true, "SELECT BIT_COUNT(1)"},

		// select time
		{"select current_timestamp", true, "SELECT CURRENT_TIMESTAMP()"},
		{"select current_timestamp()", true, "SELECT CURRENT_TIMESTAMP()"},
		{"select current_timestamp(6)", true, "SELECT CURRENT_TIMESTAMP(6)"},
		{"select current_timestamp(null)", false, ""},
		{"select current_timestamp(-1)", false, ""},
		{"select current_timestamp(1.0)", false, ""},
		{"select current_timestamp('2')", false, ""},
		{"select now()", true, "SELECT NOW()"},
		{"select now(6)", true, "SELECT NOW(6)"},
		{"select sysdate(), sysdate(6)", true, "SELECT SYSDATE(),SYSDATE(6)"},
		{"SELECT time('01:02:03');", true, "SELECT TIME('01:02:03')"},
		{"SELECT time('01:02:03.1')", true, "SELECT TIME('01:02:03.1')"},
		{"SELECT time('20.1')", true, "SELECT TIME('20.1')"},
		{"SELECT TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001');", true, "SELECT TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001')"},
		{"SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');", true, "SELECT TIMESTAMPDIFF(MONTH, '2003-02-01', '2003-05-01')"},
		{"SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01');", true, "SELECT TIMESTAMPDIFF(YEAR, '2002-05-01', '2001-01-01')"},
		{"SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55');", true, "SELECT TIMESTAMPDIFF(MINUTE, '2003-02-01', '2003-05-01 12:05:55')"},

		// select current_time
		{"select current_time", true, "SELECT CURRENT_TIME()"},
		{"select current_time()", true, "SELECT CURRENT_TIME()"},
		{"select current_time(6)", true, "SELECT CURRENT_TIME(6)"},
		{"select current_time(-1)", false, ""},
		{"select current_time(1.0)", false, ""},
		{"select current_time('1')", false, ""},
		{"select current_time(null)", false, ""},
		{"select curtime()", true, "SELECT CURTIME()"},
		{"select curtime(6)", true, "SELECT CURTIME(6)"},
		{"select curtime(-1)", false, ""},
		{"select curtime(1.0)", false, ""},
		{"select curtime('1')", false, ""},
		{"select curtime(null)", false, ""},

		// select utc_timestamp
		{"select utc_timestamp", true, "SELECT UTC_TIMESTAMP()"},
		{"select utc_timestamp()", true, "SELECT UTC_TIMESTAMP()"},
		{"select utc_timestamp(6)", true, "SELECT UTC_TIMESTAMP(6)"},
		{"select utc_timestamp(-1)", false, ""},
		{"select utc_timestamp(1.0)", false, ""},
		{"select utc_timestamp('1')", false, ""},
		{"select utc_timestamp(null)", false, ""},

		// select utc_time
		{"select utc_time", true, "SELECT UTC_TIME()"},
		{"select utc_time()", true, "SELECT UTC_TIME()"},
		{"select utc_time(6)", true, "SELECT UTC_TIME(6)"},
		{"select utc_time(-1)", false, ""},
		{"select utc_time(1.0)", false, ""},
		{"select utc_time('1')", false, ""},
		{"select utc_time(null)", false, ""},

		// for microsecond, second, minute, hour
		{"SELECT MICROSECOND('2009-12-31 23:59:59.000010');", true, "SELECT MICROSECOND('2009-12-31 23:59:59.000010')"},
		{"SELECT SECOND('10:05:03');", true, "SELECT SECOND('10:05:03')"},
		{"SELECT MINUTE('2008-02-03 10:05:03');", true, "SELECT MINUTE('2008-02-03 10:05:03')"},
		{"SELECT HOUR(), HOUR('10:05:03');", true, "SELECT HOUR(),HOUR('10:05:03')"},

		// for date, day, weekday
		{"SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE()", true, "SELECT CURRENT_DATE(),CURRENT_DATE(),CURDATE()"},
		{"SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE(1)", false, ""},
		{"SELECT DATEDIFF('2003-12-31', '2003-12-30');", true, "SELECT DATEDIFF('2003-12-31', '2003-12-30')"},
		{"SELECT DATE('2003-12-31 01:02:03');", true, "SELECT DATE('2003-12-31 01:02:03')"},
		{"SELECT DATE();", true, "SELECT DATE()"},
		{"SELECT DATE('2003-12-31 01:02:03', '');", true, "SELECT DATE('2003-12-31 01:02:03', '')"},
		{`SELECT DATE_FORMAT('2003-12-31 01:02:03', '%W %M %Y');`, true, "SELECT DATE_FORMAT('2003-12-31 01:02:03', '%W %M %Y')"},
		{"SELECT DAY('2007-02-03');", true, "SELECT DAY('2007-02-03')"},
		{"SELECT DAYOFMONTH('2007-02-03');", true, "SELECT DAYOFMONTH('2007-02-03')"},
		{"SELECT DAYOFWEEK('2007-02-03');", true, "SELECT DAYOFWEEK('2007-02-03')"},
		{"SELECT DAYOFYEAR('2007-02-03');", true, "SELECT DAYOFYEAR('2007-02-03')"},
		{"SELECT DAYNAME('2007-02-03');", true, "SELECT DAYNAME('2007-02-03')"},
		{"SELECT FROM_DAYS(1423);", true, "SELECT FROM_DAYS(1423)"},
		{"SELECT WEEKDAY('2007-02-03');", true, "SELECT WEEKDAY('2007-02-03')"},

		// for utc_date
		{"SELECT UTC_DATE, UTC_DATE();", true, "SELECT UTC_DATE(),UTC_DATE()"},
		{"SELECT UTC_DATE(), UTC_DATE()+0", true, "SELECT UTC_DATE(),UTC_DATE()+0"},

		// for week, month, year
		{"SELECT WEEK();", true, "SELECT WEEK()"},
		{"SELECT WEEK('2007-02-03');", true, "SELECT WEEK('2007-02-03')"},
		{"SELECT WEEK('2007-02-03', 0);", true, "SELECT WEEK('2007-02-03', 0)"},
		{"SELECT WEEKOFYEAR('2007-02-03');", true, "SELECT WEEKOFYEAR('2007-02-03')"},
		{"SELECT MONTH('2007-02-03');", true, "SELECT MONTH('2007-02-03')"},
		{"SELECT MONTHNAME('2007-02-03');", true, "SELECT MONTHNAME('2007-02-03')"},
		{"SELECT YEAR('2007-02-03');", true, "SELECT YEAR('2007-02-03')"},
		{"SELECT YEARWEEK('2007-02-03');", true, "SELECT YEARWEEK('2007-02-03')"},
		{"SELECT YEARWEEK('2007-02-03', 0);", true, "SELECT YEARWEEK('2007-02-03', 0)"},

		// for ADDTIME, SUBTIME
		{"SELECT ADDTIME('01:00:00.999999', '02:00:00.999998');", true, "SELECT ADDTIME('01:00:00.999999', '02:00:00.999998')"},
		{"SELECT ADDTIME('02:00:00.999998');", true, "SELECT ADDTIME('02:00:00.999998')"},
		{"SELECT ADDTIME();", true, "SELECT ADDTIME()"},
		{"SELECT SUBTIME('01:00:00.999999', '02:00:00.999998');", true, "SELECT SUBTIME('01:00:00.999999', '02:00:00.999998')"},

		// for CONVERT_TZ
		{"SELECT CONVERT_TZ();", true, "SELECT CONVERT_TZ()"},
		{"SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00');", true, "SELECT CONVERT_TZ('2004-01-01 12:00:00', '+00:00', '+10:00')"},
		{"SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00', '+10:00');", true, "SELECT CONVERT_TZ('2004-01-01 12:00:00', '+00:00', '+10:00', '+10:00')"},

		// for GET_FORMAT
		{"SELECT GET_FORMAT(DATE, 'USA');", true, "SELECT GET_FORMAT(DATE, 'USA')"},
		{"SELECT GET_FORMAT(DATETIME, 'USA');", true, "SELECT GET_FORMAT(DATETIME, 'USA')"},
		{"SELECT GET_FORMAT(TIME, 'USA');", true, "SELECT GET_FORMAT(TIME, 'USA')"},
		{"SELECT GET_FORMAT(TIMESTAMP, 'USA');", true, "SELECT GET_FORMAT(DATETIME, 'USA')"},
		// for LOCALTIME, LOCALTIMESTAMP
		{"SELECT LOCALTIME(), LOCALTIME(1)", true, "SELECT LOCALTIME(),LOCALTIME(1)"},
		{"SELECT LOCALTIMESTAMP(), LOCALTIMESTAMP(2)", true, "SELECT LOCALTIMESTAMP(),LOCALTIMESTAMP(2)"},

		// for MAKEDATE, MAKETIME
		{"SELECT MAKEDATE(2011,31);", true, "SELECT MAKEDATE(2011, 31)"},
		{"SELECT MAKETIME(12,15,30);", true, "SELECT MAKETIME(12, 15, 30)"},
		{"SELECT MAKEDATE();", true, "SELECT MAKEDATE()"},
		{"SELECT MAKETIME();", true, "SELECT MAKETIME()"},

		// for PERIOD_ADD, PERIOD_DIFF
		{"SELECT PERIOD_ADD(200801,2)", true, "SELECT PERIOD_ADD(200801, 2)"},
		{"SELECT PERIOD_DIFF(200802,200703)", true, "SELECT PERIOD_DIFF(200802, 200703)"},

		// for QUARTER
		{"SELECT QUARTER('2008-04-01');", true, "SELECT QUARTER('2008-04-01')"},

		// for SEC_TO_TIME
		{"SELECT SEC_TO_TIME(2378)", true, "SELECT SEC_TO_TIME(2378)"},

		// for TIME_FORMAT
		{`SELECT TIME_FORMAT('100:00:00', '%H %k %h %I %l')`, true, "SELECT TIME_FORMAT('100:00:00', '%H %k %h %I %l')"},

		// for TIME_TO_SEC
		{"SELECT TIME_TO_SEC('22:23:00')", true, "SELECT TIME_TO_SEC('22:23:00')"},

		// for TIMESTAMPADD
		{"SELECT TIMESTAMPADD(WEEK,1,'2003-01-02');", true, "SELECT TIMESTAMPADD(WEEK, 1, '2003-01-02')"},

		// for TO_DAYS, TO_SECONDS
		{"SELECT TO_DAYS('2007-10-07')", true, "SELECT TO_DAYS('2007-10-07')"},
		{"SELECT TO_SECONDS('2009-11-29')", true, "SELECT TO_SECONDS('2009-11-29')"},

		// for LAST_DAY
		{"SELECT LAST_DAY('2003-02-05');", true, "SELECT LAST_DAY('2003-02-05')"},

		// for UTC_TIME
		{"SELECT UTC_TIME(), UTC_TIME(1)", true, "SELECT UTC_TIME(),UTC_TIME(1)"},

		// for time extract
		{`select extract(microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(second from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(SECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(minute from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MINUTE FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(hour from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(HOUR FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(week from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(WEEK FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(month from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MONTH FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(quarter from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(QUARTER FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(year from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(YEAR FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(second_microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(SECOND_MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(minute_microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MINUTE_MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(minute_second from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(MINUTE_SECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(hour_microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(HOUR_MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(hour_second from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(HOUR_SECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(hour_minute from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(HOUR_MINUTE FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day_microsecond from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY_MICROSECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day_second from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY_SECOND FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day_minute from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY_MINUTE FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(day_hour from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(DAY_HOUR FROM '2011-11-11 10:10:10.123456')"},
		{`select extract(year_month from "2011-11-11 10:10:10.123456")`, true, "SELECT EXTRACT(YEAR_MONTH FROM '2011-11-11 10:10:10.123456')"},

		// for from_unixtime
		{`select from_unixtime(1447430881)`, true, "SELECT FROM_UNIXTIME(1447430881)"},
		{`select from_unixtime(1447430881.123456)`, true, "SELECT FROM_UNIXTIME(1447430881.123456)"},
		{`select from_unixtime(1447430881.1234567)`, true, "SELECT FROM_UNIXTIME(1447430881.1234567)"},
		{`select from_unixtime(1447430881.9999999)`, true, "SELECT FROM_UNIXTIME(1447430881.9999999)"},
		{`select from_unixtime(1447430881, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881, '%Y %D %M %h:%i:%s %x')"},
		{`select from_unixtime(1447430881.123456, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881.123456, '%Y %D %M %h:%i:%s %x')"},
		{`select from_unixtime(1447430881.1234567, "%Y %D %M %h:%i:%s %x")`, true, "SELECT FROM_UNIXTIME(1447430881.1234567, '%Y %D %M %h:%i:%s %x')"},

		// for issue 224
		{`SELECT CAST('test collated returns' AS CHAR CHARACTER SET utf8) COLLATE utf8_bin;`, true, "SELECT CAST('test collated returns' AS CHAR CHARSET UTF8)"},

		// for string functions
		// trim
		{`SELECT TRIM('  bar   ');`, true, "SELECT TRIM('  bar   ')"},
		{`SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');`, true, "SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx')"},
		{`SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');`, true, "SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx')"},
		{`SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');`, true, "SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz')"},
		{`SELECT LTRIM(' foo ');`, true, "SELECT LTRIM(' foo ')"},
		{`SELECT RTRIM(' bar ');`, true, "SELECT RTRIM(' bar ')"},

		{`SELECT RPAD('hi', 6, 'c');`, true, "SELECT RPAD('hi', 6, 'c')"},
		{`SELECT BIT_LENGTH('hi');`, true, "SELECT BIT_LENGTH('hi')"},
		{`SELECT CHAR(65);`, true, "SELECT CHAR_FUNC(65, NULL)"},
		{`SELECT CHAR_LENGTH('abc');`, true, "SELECT CHAR_LENGTH('abc')"},
		{`SELECT CHARACTER_LENGTH('abc');`, true, "SELECT CHARACTER_LENGTH('abc')"},
		{`SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo');`, true, "SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo')"},
		{`SELECT FIND_IN_SET('foo', 'foo,bar')`, true, "SELECT FIND_IN_SET('foo', 'foo,bar')"},
		{`SELECT FIND_IN_SET('foo')`, true, "SELECT FIND_IN_SET('foo')"}, // illegal number of argument still pass
		{`SELECT MAKE_SET(1,'a'), MAKE_SET(1,'a','b','c')`, true, "SELECT MAKE_SET(1, 'a'),MAKE_SET(1, 'a', 'b', 'c')"},
		{`SELECT MID('Sakila', -5, 3)`, true, "SELECT MID('Sakila', -5, 3)"},
		{`SELECT OCT(12)`, true, "SELECT OCT(12)"},
		{`SELECT OCTET_LENGTH('text')`, true, "SELECT OCTET_LENGTH('text')"},
		{`SELECT ORD('2')`, true, "SELECT ORD('2')"},
		{`SELECT POSITION('bar' IN 'foobarbar')`, true, "SELECT POSITION('bar' IN 'foobarbar')"},
		{`SELECT QUOTE('Don\'t!')`, true, "SELECT QUOTE('Don''t!')"},
		{`SELECT BIN(12)`, true, "SELECT BIN(12)"},
		{`SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo')`, true, "SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo')"},
		{`SELECT EXPORT_SET(5,'Y','N'), EXPORT_SET(5,'Y','N',','), EXPORT_SET(5,'Y','N',',',4)`, true, "SELECT EXPORT_SET(5, 'Y', 'N'),EXPORT_SET(5, 'Y', 'N', ','),EXPORT_SET(5, 'Y', 'N', ',', 4)"},
		{`SELECT FORMAT(), FORMAT(12332.2,2,'de_DE'), FORMAT(12332.123456, 4)`, true, "SELECT FORMAT(),FORMAT(12332.2, 2, 'de_DE'),FORMAT(12332.123456, 4)"},
		{`SELECT FROM_BASE64('abc')`, true, "SELECT FROM_BASE64('abc')"},
		{`SELECT TO_BASE64('abc')`, true, "SELECT TO_BASE64('abc')"},
		{`SELECT INSERT(), INSERT('Quadratic', 3, 4, 'What'), INSTR('foobarbar', 'bar')`, true, "SELECT INSERT_FUNC(),INSERT_FUNC('Quadratic', 3, 4, 'What'),INSTR('foobarbar', 'bar')"},
		{`SELECT LOAD_FILE('/tmp/picture')`, true, "SELECT LOAD_FILE('/tmp/picture')"},
		{`SELECT LPAD('hi',4,'??')`, true, "SELECT LPAD('hi', 4, '??')"},
		{`SELECT LEFT("foobar", 3)`, true, "SELECT LEFT('foobar', 3)"},
		{`SELECT RIGHT("foobar", 3)`, true, "SELECT RIGHT('foobar', 3)"},

		// repeat
		{`SELECT REPEAT("a", 10);`, true, "SELECT REPEAT('a', 10)"},

		// for miscellaneous functions
		{`SELECT SLEEP(10);`, true, "SELECT SLEEP(10)"},
		{`SELECT ANY_VALUE(@arg);`, true, "SELECT ANY_VALUE(@`arg`)"},
		{`SELECT INET_ATON('10.0.5.9');`, true, "SELECT INET_ATON('10.0.5.9')"},
		{`SELECT INET_NTOA(167773449);`, true, "SELECT INET_NTOA(167773449)"},
		{`SELECT INET6_ATON('fdfe::5a55:caff:fefa:9089');`, true, "SELECT INET6_ATON('fdfe::5a55:caff:fefa:9089')"},
		{`SELECT INET6_NTOA(INET_NTOA(167773449));`, true, "SELECT INET6_NTOA(INET_NTOA(167773449))"},
		{`SELECT IS_FREE_LOCK(@str);`, true, "SELECT IS_FREE_LOCK(@`str`)"},
		{`SELECT IS_IPV4('10.0.5.9');`, true, "SELECT IS_IPV4('10.0.5.9')"},
		{`SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'));`, true, "SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'))"},
		{`SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'));`, true, "SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'))"},
		{`SELECT IS_IPV6('10.0.5.9');`, true, "SELECT IS_IPV6('10.0.5.9')"},
		{`SELECT IS_USED_LOCK(@str);`, true, "SELECT IS_USED_LOCK(@`str`)"},
		{`SELECT MASTER_POS_WAIT(@log_name, @log_pos), MASTER_POS_WAIT(@log_name, @log_pos, @timeout), MASTER_POS_WAIT(@log_name, @log_pos, @timeout, @channel_name);`, true, "SELECT MASTER_POS_WAIT(@`log_name`, @`log_pos`),MASTER_POS_WAIT(@`log_name`, @`log_pos`, @`timeout`),MASTER_POS_WAIT(@`log_name`, @`log_pos`, @`timeout`, @`channel_name`)"},
		{`SELECT NAME_CONST('myname', 14);`, true, "SELECT NAME_CONST('myname', 14)"},
		{`SELECT RELEASE_ALL_LOCKS();`, true, "SELECT RELEASE_ALL_LOCKS()"},
		{`SELECT UUID();`, true, "SELECT UUID()"},
		{`SELECT UUID_SHORT()`, true, "SELECT UUID_SHORT()"},
		// test illegal arguments
		{`SELECT SLEEP();`, true, "SELECT SLEEP()"},
		{`SELECT ANY_VALUE();`, true, "SELECT ANY_VALUE()"},
		{`SELECT INET_ATON();`, true, "SELECT INET_ATON()"},
		{`SELECT INET_NTOA();`, true, "SELECT INET_NTOA()"},
		{`SELECT INET6_ATON();`, true, "SELECT INET6_ATON()"},
		{`SELECT INET6_NTOA(INET_NTOA());`, true, "SELECT INET6_NTOA(INET_NTOA())"},
		{`SELECT IS_FREE_LOCK();`, true, "SELECT IS_FREE_LOCK()"},
		{`SELECT IS_IPV4();`, true, "SELECT IS_IPV4()"},
		{`SELECT IS_IPV4_COMPAT(INET6_ATON());`, true, "SELECT IS_IPV4_COMPAT(INET6_ATON())"},
		{`SELECT IS_IPV4_MAPPED(INET6_ATON());`, true, "SELECT IS_IPV4_MAPPED(INET6_ATON())"},
		{`SELECT IS_IPV6()`, true, "SELECT IS_IPV6()"},
		{`SELECT IS_USED_LOCK();`, true, "SELECT IS_USED_LOCK()"},
		{`SELECT MASTER_POS_WAIT();`, true, "SELECT MASTER_POS_WAIT()"},
		{`SELECT NAME_CONST();`, true, "SELECT NAME_CONST()"},
		{`SELECT RELEASE_ALL_LOCKS(1);`, true, "SELECT RELEASE_ALL_LOCKS(1)"},
		{`SELECT UUID(1);`, true, "SELECT UUID(1)"},
		{`SELECT UUID_SHORT(1)`, true, "SELECT UUID_SHORT(1)"},
		// interval
		{`select "2011-11-11 10:10:10.123456" + interval 10 second`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select "2011-11-11 10:10:10.123456" - interval 10 second`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		// for date_add
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 hour)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 day)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 week)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 month)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 year)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10.10' SECOND_MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10.10' MINUTE_MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10' MINUTE_SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10:10.10' HOUR_MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10:10' HOUR_SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '10:10' HOUR_MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10.10' DAY_MICROSECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10' DAY_SECOND)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11 10:10' DAY_MINUTE)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11 10' DAY_HOUR)"},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, "SELECT DATE_ADD('2011-11-11 10:10:10.123456', INTERVAL '11-11' YEAR_MONTH)"},
		{`select date_add("2011-11-11 10:10:10.123456", 10)`, false, ""},
		{`select date_add("2011-11-11 10:10:10.123456", 0.10)`, false, ""},
		{`select date_add("2011-11-11 10:10:10.123456", "11,11")`, false, ""},

		// for strcmp
		{`select strcmp('abc', 'def')`, true, "SELECT STRCMP('abc', 'def')"},

		// for adddate
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 second)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 minute)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 hour)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 day)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 week)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 month)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 year)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10.10' SECOND_MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10.10' MINUTE_MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10' MINUTE_SECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10:10.10' HOUR_MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10:10' HOUR_SECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10' HOUR_MINUTE)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10.10' DAY_MICROSECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10' DAY_SECOND)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10' DAY_MINUTE)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10' DAY_HOUR)"},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11-11' YEAR_MONTH)"},
		{`select adddate("2011-11-11 10:10:10.123456", 10)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select adddate("2011-11-11 10:10:10.123456", 0.10)`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL 0.10 DAY)"},
		{`select adddate("2011-11-11 10:10:10.123456", "11,11")`, true, "SELECT ADDDATE('2011-11-11 10:10:10.123456', INTERVAL '11,11' DAY)"},

		// for date_sub
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 second)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 minute)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 hour)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 day)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 week)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 month)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 year)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10.10' SECOND_MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10.10' MINUTE_MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10' MINUTE_SECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10:10.10' HOUR_MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10:10' HOUR_SECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '10:10' HOUR_MINUTE)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10.10' DAY_MICROSECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10' DAY_SECOND)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11 10:10' DAY_MINUTE)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11 10' DAY_HOUR)"},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, "SELECT DATE_SUB('2011-11-11 10:10:10.123456', INTERVAL '11-11' YEAR_MONTH)"},
		{`select date_sub("2011-11-11 10:10:10.123456", 10)`, false, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", 0.10)`, false, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", "11,11")`, false, ""},

		// for subdate
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 second)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 SECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 minute)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 MINUTE)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 hour)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 HOUR)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 day)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 week)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 1 WEEK)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 month)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 1 MONTH)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 1 QUARTER)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 year)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 1 YEAR)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10.10' SECOND_MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10.10' MINUTE_MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10' MINUTE_SECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10:10.10' HOUR_MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10:10' HOUR_SECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '10:10' HOUR_MINUTE)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10.10 HOUR_MINUTE)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10.10' DAY_MICROSECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10:10' DAY_SECOND)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10:10' DAY_MINUTE)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11 10' DAY_HOUR)"},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11-11' YEAR_MONTH)"},
		{`select subdate("2011-11-11 10:10:10.123456", 10)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 10 DAY)"},
		{`select subdate("2011-11-11 10:10:10.123456", 0.10)`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL 0.10 DAY)"},
		{`select subdate("2011-11-11 10:10:10.123456", "11,11")`, true, "SELECT SUBDATE('2011-11-11 10:10:10.123456', INTERVAL '11,11' DAY)"},

		// for unix_timestamp
		{`select unix_timestamp()`, true, "SELECT UNIX_TIMESTAMP()"},
		{`select unix_timestamp('2015-11-13 10:20:19.012')`, true, "SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19.012')"},

		// for misc functions
		{`SELECT GET_LOCK('lock1',10);`, true, "SELECT GET_LOCK('lock1', 10)"},
		{`SELECT RELEASE_LOCK('lock1');`, true, "SELECT RELEASE_LOCK('lock1')"},

		// for aggregate functions
		{`select avg(), avg(c1,c2) from t;`, false, "SELECT AVG(),AVG(`c1`, `c2`) FROM `t`"},
		{`select avg(distinct c1) from t;`, true, "SELECT AVG(DISTINCT `c1`) FROM `t`"},
		{`select avg(distinctrow c1) from t;`, true, "SELECT AVG(DISTINCT `c1`) FROM `t`"},
		{`select avg(distinct all c1) from t;`, true, "SELECT AVG(DISTINCT `c1`) FROM `t`"},
		{`select avg(distinctrow all c1) from t;`, true, "SELECT AVG(DISTINCT `c1`) FROM `t`"},
		{`select avg(c2) from t;`, true, "SELECT AVG(`c2`) FROM `t`"},
		{`select bit_and(c1) from t;`, true, "SELECT BIT_AND(`c1`) FROM `t`"},
		{`select bit_and(all c1) from t;`, true, "SELECT BIT_AND(`c1`) FROM `t`"},
		{`select bit_and(distinct c1) from t;`, false, ""},
		{`select bit_and(distinctrow c1) from t;`, false, ""},
		{`select bit_and(distinctrow all c1) from t;`, false, ""},
		{`select bit_and(distinct all c1) from t;`, false, ""},
		{`select bit_and(), bit_and(distinct c1) from t;`, false, ""},
		{`select bit_and(), bit_and(distinctrow c1) from t;`, false, ""},
		{`select bit_and(), bit_and(all c1) from t;`, false, ""},
		{`select bit_or(c1) from t;`, true, "SELECT BIT_OR(`c1`) FROM `t`"},
		{`select bit_or(all c1) from t;`, true, "SELECT BIT_OR(`c1`) FROM `t`"},
		{`select bit_or(distinct c1) from t;`, false, ""},
		{`select bit_or(distinctrow c1) from t;`, false, ""},
		{`select bit_or(distinctrow all c1) from t;`, false, ""},
		{`select bit_or(distinct all c1) from t;`, false, ""},
		{`select bit_or(), bit_or(distinct c1) from t;`, false, ""},
		{`select bit_or(), bit_or(distinctrow c1) from t;`, false, ""},
		{`select bit_or(), bit_or(all c1) from t;`, false, ""},
		{`select bit_xor(c1) from t;`, true, "SELECT BIT_XOR(`c1`) FROM `t`"},
		{`select bit_xor(all c1) from t;`, true, "SELECT BIT_XOR(`c1`) FROM `t`"},
		{`select bit_xor(distinct c1) from t;`, false, ""},
		{`select bit_xor(distinctrow c1) from t;`, false, ""},
		{`select bit_xor(distinctrow all c1) from t;`, false, ""},
		{`select bit_xor(), bit_xor(distinct c1) from t;`, false, ""},
		{`select bit_xor(), bit_xor(distinctrow c1) from t;`, false, ""},
		{`select bit_xor(), bit_xor(all c1) from t;`, false, ""},
		{`select max(c1,c2) from t;`, false, ""},
		{`select max(distinct c1) from t;`, true, "SELECT MAX(DISTINCT `c1`) FROM `t`"},
		{`select max(distinctrow c1) from t;`, true, "SELECT MAX(DISTINCT `c1`) FROM `t`"},
		{`select max(distinct all c1) from t;`, true, "SELECT MAX(DISTINCT `c1`) FROM `t`"},
		{`select max(distinctrow all c1) from t;`, true, "SELECT MAX(DISTINCT `c1`) FROM `t`"},
		{`select max(c2) from t;`, true, "SELECT MAX(`c2`) FROM `t`"},
		{`select min(c1,c2) from t;`, false, ""},
		{`select min(distinct c1) from t;`, true, "SELECT MIN(DISTINCT `c1`) FROM `t`"},
		{`select min(distinctrow c1) from t;`, true, "SELECT MIN(DISTINCT `c1`) FROM `t`"},
		{`select min(distinct all c1) from t;`, true, "SELECT MIN(DISTINCT `c1`) FROM `t`"},
		{`select min(distinctrow all c1) from t;`, true, "SELECT MIN(DISTINCT `c1`) FROM `t`"},
		{`select min(c2) from t;`, true, "SELECT MIN(`c2`) FROM `t`"},
		{`select sum(c1,c2) from t;`, false, ""},
		{`select sum(distinct c1) from t;`, true, "SELECT SUM(DISTINCT `c1`) FROM `t`"},
		{`select sum(distinctrow c1) from t;`, true, "SELECT SUM(DISTINCT `c1`) FROM `t`"},
		{`select sum(distinct all c1) from t;`, true, "SELECT SUM(DISTINCT `c1`) FROM `t`"},
		{`select sum(distinctrow all c1) from t;`, true, "SELECT SUM(DISTINCT `c1`) FROM `t`"},
		{`select sum(c2) from t;`, true, "SELECT SUM(`c2`) FROM `t`"},
		{`select count(c1) from t;`, true, "SELECT COUNT(`c1`) FROM `t`"},
		{`select count(distinct *) from t;`, false, ""},
		{`select count(distinctrow *) from t;`, false, ""},
		{`select count(*) from t;`, true, "SELECT COUNT(1) FROM `t`"},
		{`select count(distinct c1, c2) from t;`, true, "SELECT COUNT(DISTINCT `c1`, `c2`) FROM `t`"},
		{`select count(distinctrow c1, c2) from t;`, true, "SELECT COUNT(DISTINCT `c1`, `c2`) FROM `t`"},
		{`select count(c1, c2) from t;`, false, ""},
		{`select count(all c1) from t;`, true, "SELECT COUNT(`c1`) FROM `t`"},
		{`select count(distinct all c1) from t;`, false, ""},
		{`select count(distinctrow all c1) from t;`, false, ""},
		{`select group_concat(c2,c1) from t group by c1;`, true, "SELECT GROUP_CONCAT(`c2`, `c1` SEPARATOR ',') FROM `t` GROUP BY `c1`"},
		{`select group_concat(c2,c1 SEPARATOR ';') from t group by c1;`, true, "SELECT GROUP_CONCAT(`c2`, `c1` SEPARATOR ';') FROM `t` GROUP BY `c1`"},
		{`select group_concat(distinct c2,c1) from t group by c1;`, true, "SELECT GROUP_CONCAT(DISTINCT `c2`, `c1` SEPARATOR ',') FROM `t` GROUP BY `c1`"},
		{`select group_concat(distinctrow c2,c1) from t group by c1;`, true, "SELECT GROUP_CONCAT(DISTINCT `c2`, `c1` SEPARATOR ',') FROM `t` GROUP BY `c1`"},
		{`SELECT student_name, GROUP_CONCAT(DISTINCT test_score ORDER BY test_score DESC SEPARATOR ' ') FROM student GROUP BY student_name;`, true, "SELECT `student_name`,GROUP_CONCAT(DISTINCT `test_score` ORDER BY `test_score` DESC SEPARATOR ' ') FROM `student` GROUP BY `student_name`"},
		// {`select std(c1), std(all c1), std(distinct c1) from t`, true, "SELECT STD(`c1`),STD(`c1`),STD(DISTINCT `c1`) FROM `t`"},
		// {`select std(c1, c2) from t`, false, ""},
		// {`select stddev(c1), stddev(all c1), stddev(distinct c1) from t`, true, "SELECT STDDEV(`c1`),STDDEV(`c1`),STDDEV(DISTINCT `c1`) FROM `t`"},
		// {`select stddev(c1, c2) from t`, false, ""},
		// {`select stddev_pop(c1), stddev_pop(all c1), stddev_pop(distinct c1) from t`, true, "SELECT STDDEV_POP(`c1`),STDDEV_POP(`c1`),STDDEV_POP(DISTINCT `c1`) FROM `t`"},
		// {`select stddev_pop(c1, c2) from t`, false, ""},
		// {`select stddev_samp(c1), stddev_samp(all c1), stddev_samp(distinct c1) from t`, true, "SELECT STDDEV_SAMP(`c1`),STDDEV_SAMP(`c1`),STDDEV_SAMP(DISTINCT `c1`) FROM `t`"},
		// {`select stddev_samp(c1, c2) from t`, false, ""},
		// {`select variance(c1), variance(all c1), variance(distinct c1) from t`, true, "SELECT VAR_POP(`c1`),VAR_POP(`c1`),VAR_POP(DISTINCT `c1`) FROM `t`"},
		// {`select variance(c1, c2) from t`, false, ""},
		// {`select var_pop(c1), var_pop(all c1), var_pop(distinct c1) from t`, true, "SELECT VAR_POP(`c1`),VAR_POP(`c1`),VAR_POP(DISTINCT `c1`) FROM `t`"},
		// {`select var_pop(c1, c2) from t`, false, ""},
		// {`select var_samp(c1), var_samp(all c1), var_samp(distinct c1) from t`, true, "SELECT VAR_SAMP(`c1`),VAR_SAMP(`c1`),VAR_SAMP(DISTINCT `c1`) FROM `t`"},
		// {`select var_samp(c1, c2) from t`, false, ""},
		{`select json_arrayagg(c2) from t group by c1`, true, "SELECT JSON_ARRAYAGG(`c2`) FROM `t` GROUP BY `c1`"},
		{`select json_arrayagg(c1, c2) from t group by c1`, false, ""},
		{`select json_arrayagg(distinct c2) from t group by c1`, false, "SELECT JSON_ARRAYAGG(DISTINCT `c2`) FROM `t` GROUP BY `c1`"},
		{`select json_arrayagg(all c2) from t group by c1`, true, "SELECT JSON_ARRAYAGG(`c2`) FROM `t` GROUP BY `c1`"},
		{`select json_objectagg(c1, c2) from t group by c1`, true, "SELECT JSON_OBJECTAGG(`c1`, `c2`) FROM `t` GROUP BY `c1`"},
		{`select json_objectagg(c1, c2, c3) from t group by c1`, false, ""},
		{`select json_objectagg(distinct c1, c2) from t group by c1`, false, "SELECT JSON_OBJECTAGG(DISTINCT `c1`, `c2`) FROM `t` GROUP BY `c1`"},
		{`select json_objectagg(c1, distinct c2) from t group by c1`, false, "SELECT JSON_OBJECTAGG(`c1`, DISTINCT `c2`) FROM `t` GROUP BY `c1`"},
		{`select json_objectagg(distinct c1, distinct c2) from t group by c1`, false, "SELECT JSON_OBJECTAGG(DISTINCT `c1`, DISTINCT `c2`) FROM `t` GROUP BY `c1`"},
		{`select json_objectagg(all c1, c2) from t group by c1`, true, "SELECT JSON_OBJECTAGG(`c1`, `c2`) FROM `t` GROUP BY `c1`"},
		{`select json_objectagg(c1, all c2) from t group by c1`, true, "SELECT JSON_OBJECTAGG(`c1`, `c2`) FROM `t` GROUP BY `c1`"},
		{`select json_objectagg(all c1, all c2) from t group by c1`, true, "SELECT JSON_OBJECTAGG(`c1`, `c2`) FROM `t` GROUP BY `c1`"},

		// for encryption and compression functions
		{`select AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3'))`, true, "SELECT AES_ENCRYPT('text', UNHEX('F3229A0B371ED2D9441B830D21A390C3'))"},
		{`select AES_DECRYPT(@crypt_str,@key_str)`, true, "SELECT AES_DECRYPT(@`crypt_str`, @`key_str`)"},
		{`select AES_DECRYPT(@crypt_str,@key_str,@init_vector);`, true, "SELECT AES_DECRYPT(@`crypt_str`, @`key_str`, @`init_vector`)"},
		{`SELECT COMPRESS('');`, true, "SELECT COMPRESS('')"},
		{`SELECT DECODE(@crypt_str, @pass_str);`, true, "SELECT DECODE(@`crypt_str`, @`pass_str`)"},
		{`SELECT DES_DECRYPT(@crypt_str), DES_DECRYPT(@crypt_str, @key_str);`, true, "SELECT DES_DECRYPT(@`crypt_str`),DES_DECRYPT(@`crypt_str`, @`key_str`)"},
		{`SELECT DES_ENCRYPT(@str), DES_ENCRYPT(@key_num);`, true, "SELECT DES_ENCRYPT(@`str`),DES_ENCRYPT(@`key_num`)"},
		{`SELECT ENCODE('cleartext', CONCAT('my_random_salt','my_secret_password'));`, true, "SELECT ENCODE('cleartext', CONCAT('my_random_salt', 'my_secret_password'))"},
		{`SELECT ENCRYPT('hello'), ENCRYPT('hello', @salt);`, true, "SELECT ENCRYPT('hello'),ENCRYPT('hello', @`salt`)"},
		{`SELECT MD5('testing');`, true, "SELECT MD5('testing')"},
		{`SELECT OLD_PASSWORD(@str);`, true, "SELECT OLD_PASSWORD(@`str`)"},
		{`SELECT PASSWORD(@str);`, true, "SELECT PASSWORD_FUNC(@`str`)"},
		{`SELECT RANDOM_BYTES(@len);`, true, "SELECT RANDOM_BYTES(@`len`)"},
		{`SELECT SHA1('abc');`, true, "SELECT SHA1('abc')"},
		{`SELECT SHA('abc');`, true, "SELECT SHA('abc')"},
		{`SELECT SHA2('abc', 224);`, true, "SELECT SHA2('abc', 224)"},
		{`SELECT UNCOMPRESS('any string');`, true, "SELECT UNCOMPRESS('any string')"},
		{`SELECT UNCOMPRESSED_LENGTH(@compressed_string);`, true, "SELECT UNCOMPRESSED_LENGTH(@`compressed_string`)"},
		{`SELECT VALIDATE_PASSWORD_STRENGTH(@str);`, true, "SELECT VALIDATE_PASSWORD_STRENGTH(@`str`)"},

		// For JSON functions.
		{`SELECT JSON_EXTRACT();`, true, "SELECT JSON_EXTRACT()"},
		{`SELECT JSON_UNQUOTE();`, true, "SELECT JSON_UNQUOTE()"},
		{`SELECT JSON_TYPE('[123]');`, true, "SELECT JSON_TYPE('[123]')"},
		{`SELECT JSON_TYPE();`, true, "SELECT JSON_TYPE()"},

		// For two json grammar sugar.
		{`SELECT a->'$.a' FROM t`, true, "SELECT JSON_EXTRACT(`a`, '$.a') FROM `t`"},
		{`SELECT a->>'$.a' FROM t`, true, "SELECT JSON_UNQUOTE(JSON_EXTRACT(`a`, '$.a')) FROM `t`"},
		{`SELECT '{}'->'$.a' FROM t`, false, ""},
		{`SELECT '{}'->>'$.a' FROM t`, false, ""},
		{`SELECT a->3 FROM t`, false, ""},
		{`SELECT a->>3 FROM t`, false, ""},

		// Test that quoted identifier can be a function name.
		{"SELECT `uuid`()", true, "SELECT UUID()"},
		// Test sequence function.
		{"select nextval(seq)", true, "SELECT NEXTVAL(`seq`)"},
		{"select lastval(seq)", true, "SELECT LASTVAL(`seq`)"},
		{"select setval(seq, 100)", true, "SELECT SETVAL(`seq`, 100)"},
		{"select next value for seq", true, "SELECT NEXTVAL(`seq`)"},
		{"select next value for seq", true, "SELECT NEXTVAL(`seq`)"},
		{"select NeXt vAluE for seQuEncE2", true, "SELECT NEXTVAL(`seQuEncE2`)"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestIdentifier(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// for quote identifier
		{"select `a`, `a.b`, `a b` from t", true, "SELECT `a`,`a.b`,`a b` FROM `t`"},
		// for unquoted identifier
		{"create table MergeContextTest$Simple (value integer not null, primary key (value))", true, "CREATE TABLE `MergeContextTest$Simple` (`value` INT NOT NULL,PRIMARY KEY(`value`))"},
		// for as
		{"select 1 as a, 1 as `a`, 1 as \"a\", 1 as 'a'", true, "SELECT 1 AS `a`,1 AS `a`,1 AS `a`,1 AS `a`"},
		{`select 1 as a, 1 as "a", 1 as 'a'`, true, "SELECT 1 AS `a`,1 AS `a`,1 AS `a`"},
		{`select 1 a, 1 "a", 1 'a'`, true, "SELECT 1 AS `a`,1 AS `a`,1 AS `a`"},
		{`select * from t as "a"`, false, ""},
		{`select * from t a`, true, "SELECT * FROM `t` AS `a`"},
		// reserved keyword can't be used as identifier directly, but A.B pattern is an exception
		{`select COUNT from DESC`, false, ""},
		{`select COUNT from SELECT.DESC`, true, "SELECT `COUNT` FROM `SELECT`.`DESC`"},
		{"use `select`", true, "USE `select`"},
		{"use `sel``ect`", true, "USE `sel``ect`"},
		{"use select", false, "USE `select`"},
		{`select * from t as a`, true, "SELECT * FROM `t` AS `a`"},
		// {"select 1 full, 1 row, 1 abs", false, ""},
		{"select 1 full, 1 `row`, 1 abs", true, "SELECT 1 AS `full`,1 AS `row`,1 AS `abs`"},
		// {"select * from t full, t1 row, t2 abs", false, ""},
		{"select * from t full, t1 `row`, t2 abs", true, "SELECT * FROM ((`t` AS `full`) JOIN `t1` AS `row`) JOIN `t2` AS `abs`"},
		// for issue 1878, identifiers may begin with digit.
		{"create database 123test", true, "CREATE DATABASE `123test`"},
		{"create database 123", false, "CREATE DATABASE `123`"},
		{"create database `123`", true, "CREATE DATABASE `123`"},
		{"create database `12``3`", true, "CREATE DATABASE `12``3`"},
		{"create table `123` (123a1 int)", true, "CREATE TABLE `123` (`123a1` INT)"},
		{"create table 123 (123a1 int)", false, ""},
		{fmt.Sprintf("select * from t%cble", 0), false, ""},
		// for issue 3954, should NOT be recognized as identifiers.
		{`select .78+123`, true, "SELECT 0.78+123"},
		{`select .78+.21`, true, "SELECT 0.78+0.21"},
		{`select .78-123`, true, "SELECT 0.78-123"},
		{`select .78-.21`, true, "SELECT 0.78-0.21"},
		{`select .78--123`, true, "SELECT 0.78--123"},
		{`select .78*123`, true, "SELECT 0.78*123"},
		{`select .78*.21`, true, "SELECT 0.78*0.21"},
		{`select .78/123`, true, "SELECT 0.78/123"},
		{`select .78/.21`, true, "SELECT 0.78/0.21"},
		{`select .78,123`, true, "SELECT 0.78,123"},
		{`select .78,.21`, true, "SELECT 0.78,0.21"},
		{`select .78 , 123`, true, "SELECT 0.78,123"},
		{`select .78.123`, false, ""},
		{`select .78#123`, true, "SELECT 0.78"},
		{`insert float_test values(.67, 'string');`, true, "INSERT INTO `float_test` VALUES (0.67,'string')"},
		{`select .78'123'`, true, "SELECT 0.78 AS `123`"},
		{"select .78`123`", true, "SELECT 0.78 AS `123`"},
		{`select .78"123"`, true, "SELECT 0.78 AS `123`"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestDDL(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{"CREATE", false, ""},
		{"CREATE TABLE", false, ""},
		{"CREATE TABLE foo (", false, ""},
		{"CREATE TABLE foo ()", false, ""},
		{"CREATE TABLE foo ();", false, ""},
		{"CREATE TABLE foo.* (a varchar(50), b int);", false, ""},
		{"CREATE TABLE foo (a varchar(50), b int);", true, "CREATE TABLE `foo` (`a` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE foo (a TINYINT UNSIGNED);", true, "CREATE TABLE `foo` (`a` TINYINT UNSIGNED)"},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo (a bigint unsigned, b bool);", true, "CREATE TABLE `foo` (`a` BIGINT UNSIGNED,`b` TINYINT(1))"},
		{"CREATE TABLE foo (a TINYINT, b SMALLINT) CREATE TABLE bar (x INT, y int64)", false, ""},
		{"CREATE TABLE foo (a int, b float); CREATE TABLE bar (x double, y float)", true, "CREATE TABLE `foo` (`a` INT,`b` FLOAT); CREATE TABLE `bar` (`x` DOUBLE,`y` FLOAT)"},
		{"CREATE TABLE foo (a bytes)", false, ""},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) -- foo", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) // foo", false, ""},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo /* foo */ (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true, "CREATE TABLE `foo` (`a` SMALLINT UNSIGNED,`b` INT UNSIGNED)"},
		{"CREATE TABLE foo (name CHAR(50) BINARY);", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY)"},
		{"CREATE TABLE foo (name CHAR(50) COLLATE utf8_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE utf8_bin)"},
		{"CREATE TABLE foo (id varchar(50) collate utf8_bin);", true, "CREATE TABLE `foo` (`id` VARCHAR(50) COLLATE utf8_bin)"},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET UTF8)", true, "CREATE TABLE `foo` (`name` CHAR(50) CHARACTER SET UTF8)"},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8 BINARY)", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY CHARACTER SET UTF8)"},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8 BINARY CHARACTER set utf8)", false, ""},
		{"CREATE TABLE foo (name CHAR(50) BINARY CHARACTER SET utf8 COLLATE utf8_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) BINARY CHARACTER SET UTF8 COLLATE utf8_bin)"},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8 COLLATE utf8_bin COLLATE ascii_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) CHARACTER SET UTF8 COLLATE utf8_bin COLLATE ascii_bin)"},
		{"CREATE TABLE foo (name CHAR(50) COLLATE ascii_bin COLLATE latin1_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE ascii_bin COLLATE latin1_bin)"},
		{"CREATE TABLE foo (name CHAR(50) COLLATE ascii_bin PRIMARY KEY COLLATE latin1_bin)", true, "CREATE TABLE `foo` (`name` CHAR(50) COLLATE ascii_bin PRIMARY KEY COLLATE latin1_bin)"},
		{"CREATE TABLE foo (a.b, b);", false, ""},
		{"CREATE TABLE foo (a, b.c);", false, ""},
		{"CREATE TABLE (name CHAR(50) BINARY)", false, ""},
		// test enable or disable cached table
		{"ALTER TABLE tmp CACHE", true, "ALTER TABLE `tmp` CACHE"},
		{"ALTER TABLE tmp NOCACHE", true, "ALTER TABLE `tmp` NOCACHE"},
		//for create temporary table
		{"CREATE TEMPORARY TABLE t (a varchar(50), b int);", true, "CREATE TEMPORARY TABLE `t` (`a` VARCHAR(50),`b` INT)"},
		{"CREATE TEMPORARY TABLE t LIKE t1", true, "CREATE TEMPORARY TABLE `t` LIKE `t1`"},
		{"DROP TEMPORARY TABLE t", true, "DROP TEMPORARY TABLE `t`"},
		{"create global temporary table t (a int, b varchar(255)) on commit delete rows", true, "CREATE GLOBAL TEMPORARY TABLE `t` (`a` INT,`b` VARCHAR(255)) ON COMMIT DELETE ROWS"},
		{"create temporary table t (a int, b varchar(255))", true, "CREATE TEMPORARY TABLE `t` (`a` INT,`b` VARCHAR(255))"},
		{"create global temporary table t (a int, b varchar(255))", false, ""}, // missing on commit delete rows
		{"create temporary table t (a int, b varchar(255)) on commit delete rows", false, ""},
		{"create table t (a int, b varchar(255)) on commit delete rows", false, ""},
		{"create global temporary table t (a int, b varchar(255)) partition by hash(a) partitions 10 on commit delete rows", true, "CREATE GLOBAL TEMPORARY TABLE `t` (`a` INT,`b` VARCHAR(255)) PARTITION BY HASH (`a`) PARTITIONS 10 ON COMMIT DELETE ROWS"},
		{"create global temporary table t (a int, b varchar(255)) on commit preserve rows", true, "CREATE GLOBAL TEMPORARY TABLE `t` (`a` INT,`b` VARCHAR(255)) ON COMMIT PRESERVE ROWS"},
		{"drop global temporary table t", true, "DROP GLOBAL TEMPORARY TABLE `t`"},
		{"drop temporary table t", true, "DROP TEMPORARY TABLE `t`"},
		//
		{"CREATE TABLE foo (a varchar(50), b int) TABLE_MODE = 'queuing';", true, "CREATE TABLE `foo` (`a` VARCHAR(50),`b` INT) TABLE_MODE = 'queuing'"},
		{"CREATE TABLE foo (a varchar(50), b int) SINGLE;", true, "CREATE TABLE `foo` (`a` VARCHAR(50),`b` INT) SINGLE"},
		{"CREATE TABLE foo (a varchar(50), b int) BROADCAST;", true, "CREATE TABLE `foo` (`a` VARCHAR(50),`b` INT) BROADCAST"},
		// for create partition table
		{"CREATE PARTITION TABLE t (a varchar(50), b int);", true, "CREATE PARTITION TABLE `t` (`a` VARCHAR(50),`b` INT)"},
		// for create materialized view
		{"create materialized view v as select * from t", true, "CREATE MATERIALIZED VIEW `v` AS SELECT * FROM `t`"},
		{"create materialized view v never refresh as select * from t", true, "CREATE MATERIALIZED VIEW `v` NEVER REFRESH AS SELECT * FROM `t`"},
		{"create materialized view v refresh fast start with sysdate() next sysdate() + interval 1 day as select * from t", true, "CREATE MATERIALIZED VIEW `v` REFRESH FAST START WITH SYSDATE() NEXT SYSDATE() + INTERVAL 1 DAY AS SELECT * FROM `t`"},
		{"create materialized view v partition by hash(col1) partitions 8 never refresh as select * from t", true, "CREATE MATERIALIZED VIEW `v` PARTITION BY HASH(COL1) PARTITIONS 8 NEVER REFRESH AS SELECT * FROM `t`"},
		{"create materialized view v parallel 8 partition by hash(col1) partitions 8 never refresh as select * from t", true, "CREATE MATERIALIZED VIEW `v` PARALLEL 8 PARTITION BY HASH(COL1) PARTITIONS 8 NEVER REFRESH AS SELECT * FROM `t`"},
		{"create materialized view v disable query rewrite as select * from t", true, "CREATE MATERIALIZED VIEW `v` DISABLE QUERY REWRITE AS SELECT * FROM `t`"},
		{"create materialized view v enable query rewrite as select * from t", true, "CREATE MATERIALIZED VIEW `v` ENABLE QUERY REWRITE AS SELECT * FROM `t`"},
		{"create materialized view v disable on query computation as select * from t", true, "CREATE MATERIALIZED VIEW `v` DISABLE ON QUERY COMPUTATION AS SELECT * FROM `t`"},
		{"create materialized view v disable query rewrite disable on query computation as select * from t", true, "CREATE MATERIALIZED VIEW `v` DISABLE QUERY REWRITE DISABLE ON QUERY COMPUTATION AS SELECT * FROM `t`"},
		{"create materialized view v partition by hash(col1) partitions 8 never refresh disable query rewrite disable on query computation as select * from t", true, "CREATE MATERIALIZED VIEW `v` PARTITION BY HASH(COL1) PARTITIONS 8 NEVER REFRESH DISABLE QUERY REWRITE DISABLE ON QUERY COMPUTATION AS SELECT * FROM `t`"},
		// for drop materialized view
		{"drop materialized view v", true, "DROP MATERIALIZED VIEW `v`"},
		{"drop materialized view v cascade", true, "DROP MATERIALIZED VIEW `v` CASCADE"},
		{"drop materialized view v restrict", true, "DROP MATERIALIZED VIEW `v` RESTRICT"},

		// for create materialized view log
		{"create materialized view log on v with primary key", true, "CREATE MATERIALIZED VIEW LOG ON `v` WITH PRIMARY KEY"},
		{"create materialized view log on v with sequence(name ,age)", true, "CREATE MATERIALIZED VIEW LOG ON `v` WITH SEQUENCE(`NAME` ,`AGE`)"},
		{"create materialized view log on v with sequence(name ,age) including new values", true, "CREATE MATERIALIZED VIEW LOG ON `v` WITH SEQUENCE(`NAME` ,`AGE`) INCLUDING NEW VALUES"},
		{"create materialized view log on v parallel 8 with sequence(name ,age) including new values", true, "CREATE MATERIALIZED VIEW LOG ON `v` PARALLEL 8 WITH SEQUENCE(`NAME` ,`AGE`) INCLUDING NEW VALUES"},
		{"create materialized view log on v with sequence(name ,age) including new values purge start with sysdate()", true, "CREATE MATERIALIZED VIEW LOG ON `v` WITH SEQUENCE(`NAME` ,`AGE`) INCLUDING NEW VALUES PURGE START WITH SYSDATE()"},
		{"create materialized view log on v with sequence(name ,age) including new values purge next sysdate()", true, "CREATE MATERIALIZED VIEW LOG ON `v` WITH SEQUENCE(`NAME` ,`AGE`) INCLUDING NEW VALUES PURGE NEXT SYSDATE()"},
		{"create materialized view log on v with sequence(name ,age) including new values purge start with sysdate() next sysdate() + interval 1 day", true, "CREATE MATERIALIZED VIEW LOG ON `v` WITH SEQUENCE(`NAME` ,`AGE`) INCLUDING NEW VALUES PURGE START WITH SYSDATE() NEXT SYSDATE() + INTERVAL 1 DAY"},
		// for drop materialized view log
		{"drop materialized view log on v", true, "DROP MATERIALIZED VIEW LOG ON `v`"},
		// for create sequence
		{"create sequence sequence", false, ""},
		{"create sequence seq", true, "CREATE SEQUENCE `seq`"},
		{"create sequence if not exists seq", true, "CREATE SEQUENCE IF NOT EXISTS `seq`"},
		{"create sequence if not exists seq increment", false, ""},
		{"create sequence if not exists seq increment 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1"},
		{"create sequence if not exists seq increment = 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1"},
		{"create sequence if not exists seq increment by 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1"},
		{"create sequence if not exists seq minvalue", false, ""},
		{"create sequence if not exists seq minvalue 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1"},
		{"create sequence if not exists seq minvalue = 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1"},
		{"create sequence if not exists seq no", false, ""},
		{"create sequence if not exists seq nominvalue", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE"},
		{"create sequence if not exists seq no minvalue", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE"},
		{"create sequence if not exists seq maxvalue", false, ""},
		{"create sequence if not exists seq maxvalue 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1"},
		{"create sequence if not exists seq maxvalue = 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1"},
		{"create sequence if not exists seq no", false, ""},
		{"create sequence if not exists seq nomaxvalue", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE"},
		{"create sequence if not exists seq no maxvalue", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE"},
		{"create sequence if not exists seq start", false, ""},
		{"create sequence if not exists seq start with", false, ""},
		{"create sequence if not exists seq start =", false, ""},
		{"create sequence if not exists seq start with", false, ""},
		{"create sequence if not exists seq start 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1"},
		{"create sequence if not exists seq start = 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1"},
		{"create sequence if not exists seq start with 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1"},
		{"create sequence if not exists seq cache", false, ""},
		{"create sequence if not exists seq cache 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` CACHE 1"},
		{"create sequence if not exists seq cache = 1", true, "CREATE SEQUENCE IF NOT EXISTS `seq` CACHE 1"},
		{"create sequence if not exists seq nocache", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE"},
		{"create sequence if not exists seq no cache", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE"},
		{"create sequence if not exists seq cycle", true, "CREATE SEQUENCE IF NOT EXISTS `seq` CYCLE"},
		{"create sequence if not exists seq nocycle", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE"},
		{"create sequence if not exists seq no cycle", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE"},
		{"create sequence if not exists seq order", true, "CREATE SEQUENCE IF NOT EXISTS `seq` ORDER"},
		{"create sequence if not exists seq noorder", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NOORDER"},
		{"create sequence if not exists seq no order", true, "CREATE SEQUENCE IF NOT EXISTS `seq` NOORDER"},
		{"create sequence seq increment 1 start with 0 minvalue 0 maxvalue 1000", true, "CREATE SEQUENCE `seq` INCREMENT BY 1 START WITH 0 MINVALUE 0 MAXVALUE 1000"},
		{"create sequence seq increment 1 start with 0 minvalue 0 maxvalue 1000", true, "CREATE SEQUENCE `seq` INCREMENT BY 1 START WITH 0 MINVALUE 0 MAXVALUE 1000"},
		// TODO : support or replace if need : care for it will conflict on temporary.
		{"create sequence if not exists seq increment 1 start with 0 minvalue -2 maxvalue 1000", true, "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1 START WITH 0 MINVALUE -2 MAXVALUE 1000"},
		{"create sequence seq increment -1 start with -1 minvalue -1 maxvalue -1000 cache = 10 nocycle noorder", true, "CREATE SEQUENCE `seq` INCREMENT BY -1 START WITH -1 MINVALUE -1 MAXVALUE -1000 CACHE 10 NOCYCLE NOORDER"},
		// test drop sequence
		{"drop sequence", false, ""},
		{"drop sequence seq", true, "DROP SEQUENCE `seq`"},
		{"drop sequence if exists seq", true, "DROP SEQUENCE IF EXISTS `seq`"},
		{"drop sequence seq seq2", false, ""},
		{"drop sequence seq, seq2", true, "DROP SEQUENCE `seq`, `seq2`"},
		// for alter sequence
		{"alter sequence seq", false, ""},
		{"alter sequence seq comment=\"haha\"", false, ""},
		{"alter sequence seq start = 1", true, "ALTER SEQUENCE `seq` START WITH 1"},
		{"alter sequence seq start with 1 increment by 1", true, "ALTER SEQUENCE `seq` START WITH 1 INCREMENT BY 1"},
		{"alter sequence seq start with 1 increment by 2 minvalue 0 maxvalue 100", true, "ALTER SEQUENCE `seq` START WITH 1 INCREMENT BY 2 MINVALUE 0 MAXVALUE 100"},
		{"alter sequence seq increment -1 start with -1 minvalue -1 maxvalue -1000 cache = 10 nocycle", true, "ALTER SEQUENCE `seq` INCREMENT BY -1 START WITH -1 MINVALUE -1 MAXVALUE -1000 CACHE 10 NOCYCLE"},
		{"alter sequence if exists seq2 increment = 2", true, "ALTER SEQUENCE IF EXISTS `seq2` INCREMENT BY 2"},
		{"alter sequence seq restart", true, "ALTER SEQUENCE `seq` RESTART"},
		{"alter sequence seq start with 3 restart with 5", true, "ALTER SEQUENCE `seq` START WITH 3 RESTART WITH 5"},
		{"alter sequence seq restart = 5", true, "ALTER SEQUENCE `seq` RESTART WITH 5"},
		{"create sequence seq restart = 5", false, ""},
		// for create table GLOBAL/LOCAL INDEX
		{"CREATE TABLE t (a varchar(50), b int, index idx_b(b) );", true, "CREATE TABLE `t` (`a` VARCHAR(50),`b` INT, index idx_b(b) GLOBAL)"},
		{"CREATE TABLE t (a varchar(50), b int, index idx_b(b) LOCAL);", true, "CREATE TABLE `t` (`a` VARCHAR(50),`b` INT, index idx_b(b) LOCAL)"},
		{"CREATE TABLE t (a varchar(50), b int, GLOBAL index idx_b(b) PARTITION BY HASH(b));", true, "CREATE TABLE `t` (`a` VARCHAR(50),`b` INT, GLOBAL index idx_b(`b`) PARTITION BY HASH(`b`))"},
		{"CREATE TABLE t (a varchar(50), b int, unique global index idx_b(b)  PARTITION BY HASH(b));", true, "CREATE TABLE `t` (`a` VARCHAR(50),`b` INT, UNIQUE GLOBAL index idx_b(b) PARTITION BY HASH(`b`))"},
		// column visibility option
		{"CREATE TABLE foo (pump varchar(50), b int visible);", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`b` INT VISIBLE)"},
		//parallel option
		{"CREATE TABLE foo (pump varchar(50), b int) parallel 10;", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`b` INT) PARALLEL 10"},
		{"CREATE TABLE foo (pump varchar(50), b int) noparallel 1;", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`b` INT) NOPARALLEL 10"},
		//for create table with index column group option
		{"CREATE TABLE foo (pump varchar(50), b int, index i1 (b) with column group(each column));", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`B` INT, INDEX i1 (`B`) WITH COLUMN GROUP(each column))"},
		{"CREATE TABLE foo (pump varchar(50), b int, index i1 (b) with column group(all columns, each column));", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`B` INT, INDEX i1 (`B`) WITH COLUMN GROUP(ALL COLUMNS, EACH COLUMN))"},
		//for create table with table column group option
		{"CREATE TABLE foo (pump varchar(50), b int) with column group(each column);", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`B` INT) WITH COLUMN GROUP(each column)"},
		//for alter table with add column group option
		{"ALTER TABLE foo add column group(each column);", true, "ALTER TABLE `foo` ADD COLUMN GROUP(each column)"},
		//for alter table with drop column group option
		{"ALTER TABLE foo drop column group(each column);", true, "ALTER TABLE `foo` DROP COLUMN GROUP(each column)"},
		//for create table with table column storing index
		{"CREATE TABLE foo (pump varchar(50), b int, index i1 (b) storing (b) with column group(each column));", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`b` INT, INDEX i1 (`B`) STORING (`B`) WITH COLUMN GROUP(each column))"},
		// test use key word as column name
		{"CREATE TABLE foo (pump varchar(50), b int);", true, "CREATE TABLE `foo` (`pump` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE foo (drainer varchar(50), b int);", true, "CREATE TABLE `foo` (`drainer` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE foo (node_id varchar(50), b int);", true, "CREATE TABLE `foo` (`node_id` VARCHAR(50),`b` INT)"},
		{"CREATE TABLE foo (node_state varchar(50), b int);", true, "CREATE TABLE `foo` (`node_state` VARCHAR(50),`b` INT)"},
		// for table option
		{"create table t (c int) avg_row_length = 3", true, "CREATE TABLE `t` (`c` INT) AVG_ROW_LENGTH = 3"},
		{"create table t (c int) avg_row_length 3", true, "CREATE TABLE `t` (`c` INT) AVG_ROW_LENGTH = 3"},
		{"create table t (c int) checksum = 0", true, "CREATE TABLE `t` (`c` INT) CHECKSUM = 0"},
		{"create table t (c int) checksum 1", true, "CREATE TABLE `t` (`c` INT) CHECKSUM = 1"},
		{"create table t (c int) compression = 'NONE'", true, "CREATE TABLE `t` (`c` INT) COMPRESSION = 'NONE'"},
		{"create table t (c int) compression 'lz4'", true, "CREATE TABLE `t` (`c` INT) COMPRESSION = 'lz4'"},
		{"create table t (c int) connection = 'abc'", true, "CREATE TABLE `t` (`c` INT) CONNECTION = 'abc'"},
		{"create table t (c int) connection 'abc'", true, "CREATE TABLE `t` (`c` INT) CONNECTION = 'abc'"},
		{"create table t (c int) key_block_size = 1024", true, "CREATE TABLE `t` (`c` INT) KEY_BLOCK_SIZE = 1024"},
		{"create table t (c int) key_block_size 1024", true, "CREATE TABLE `t` (`c` INT) KEY_BLOCK_SIZE = 1024"},
		{"create table t (c int) max_rows = 1000", true, "CREATE TABLE `t` (`c` INT) MAX_ROWS = 1000"},
		{"create table t (c int) max_rows 1000", true, "CREATE TABLE `t` (`c` INT) MAX_ROWS = 1000"},
		{"create table t (c int) min_rows = 1000", true, "CREATE TABLE `t` (`c` INT) MIN_ROWS = 1000"},
		{"create table t (c int) min_rows 1000", true, "CREATE TABLE `t` (`c` INT) MIN_ROWS = 1000"},
		{"create table t (c int) password = 'abc'", true, "CREATE TABLE `t` (`c` INT) PASSWORD = 'abc'"},
		{"create table t (c int) password 'abc'", true, "CREATE TABLE `t` (`c` INT) PASSWORD = 'abc'"},
		{"create table t (c int) DELAY_KEY_WRITE=1", true, "CREATE TABLE `t` (`c` INT) DELAY_KEY_WRITE = 1"},
		{"create table t (c int) DELAY_KEY_WRITE 1", true, "CREATE TABLE `t` (`c` INT) DELAY_KEY_WRITE = 1"},
		{"create table t (c int) ROW_FORMAT = default", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DEFAULT"},
		{"create table t (c int) ROW_FORMAT default", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DEFAULT"},
		{"create table t (c int) ROW_FORMAT = fixed", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = FIXED"},
		{"create table t (c int) ROW_FORMAT = compressed", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = COMPRESSED"},
		{"create table t (c int) ROW_FORMAT = compact", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = COMPACT"},
		{"create table t (c int) ROW_FORMAT = redundant", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = REDUNDANT"},
		{"create table t (c int) ROW_FORMAT = dynamic", true, "CREATE TABLE `t` (`c` INT) ROW_FORMAT = DYNAMIC"},
		{"create table t (c int) STATS_PERSISTENT = default", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table t (c int) STATS_PERSISTENT = 0", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table t (c int) STATS_PERSISTENT = 1", true, "CREATE TABLE `t` (`c` INT) STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ "},
		{"create table t (c int) PACK_KEYS = 1", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{"create table t (c int) PACK_KEYS = 0", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{"create table t (c int) PACK_KEYS = DEFAULT", true, "CREATE TABLE `t` (`c` INT) PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ "},
		{`create table testTableCompression (c VARCHAR(15000)) compression="ZLIB";`, true, "CREATE TABLE `testTableCompression` (`c` VARCHAR(15000)) COMPRESSION = 'ZLIB'"},
		{`create table t1 (c1 int) compression="zlib";`, true, "CREATE TABLE `t1` (`c1` INT) COMPRESSION = 'zlib'"},

		// partition option
		{"create table t (b int) partition by range columns (b) (partition p0 values less than (not 3), partition p2 values less than (20));", false, ""},
		{"create table t (b int) partition by range columns (b) (partition p0 values less than (1 or 3), partition p2 values less than (20));", false, ""},
		{"create table t (b int) partition by range columns (b) (partition p0 values less than (3 is null), partition p2 values less than (20));", false, ""},
		{"create table t (b int) partition by range (b is null) (partition p0 values less than (10));", false, ""},
		{"create table t (b int) partition by list (not b) (partition p0 values in (10, 20));", false, ""},
		{"create table t (b int) partition by hash ( not b );", false, ""},
		{"create table t (b int) partition by range columns (b) (partition p0 values less than (3 in (3, 4, 5)), partition p2 values less than (20));", false, ""},
		{"CREATE TABLE t (id int) ENGINE = INNDB PARTITION BY RANGE (id) INTERVAL (100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000);", true, "CREATE TABLE `t` (`id` INT) ENGINE = INNDB PARTITION BY RANGE (`id`) INTERVAL (100)  FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000)"},
		{"CREATE TABLE t (id int) ENGINE = INNDB PARTITION BY RANGE (id) INTERVAL (100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000) MAXVALUE PARTITION;", true, "CREATE TABLE `t` (`id` INT) ENGINE = INNDB PARTITION BY RANGE (`id`) INTERVAL (100)  FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000) MAXVALUE PARTITION"},

		{"CREATE TABLE t (id int) ENGINE = INNDB PARTITION BY RANGE (id) INTERVAL (MONTH) FIRST PARTITION LESS THAN ('2000-01-01') LAST PARTITION LESS THAN ('2025-01-01');", true, "CREATE TABLE `t` (`id` INT) ENGINE = INNDB PARTITION BY RANGE (`id`) INTERVAL (MONTH)  FIRST PARTITION LESS THAN ('2000-01-01') LAST PARTITION LESS THAN ('2025-01-01')"},
		{"CREATE TABLE t (id int) ENGINE = INNDB PARTITION BY RANGE (id) INTERVAL (100) FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000) MAXVALUE PARTITION;", true, "CREATE TABLE `t` (`id` INT) ENGINE = INNDB PARTITION BY RANGE (`id`) INTERVAL (100)  FIRST PARTITION LESS THAN (100) LAST PARTITION LESS THAN (10000) MAXVALUE PARTITION"},

		{"CREATE TABLE t (id int) ENGINE = INNDB PARTITION BY RANGE (id) INTERVAL (1 MONTH) FIRST PARTITION LESS THAN ('2000-01-01') LAST PARTITION LESS THAN ('2025-01-01');", true, "CREATE TABLE `t` (`id` INT) ENGINE = INNDB PARTITION BY RANGE (`id`) INTERVAL (1 MONTH)  FIRST PARTITION LESS THAN ('2000-01-01') LAST PARTITION LESS THAN ('2025-01-01')"},

		{"CREATE TABLE t (id int) ENGINE = INNDB PARTITION BY RANGE (id) INTERVAL (MONTH, 1)  (PARTITION p20211101 VALUES LESS THAN ('2021-11-01 00:00:00'));", true, "CREATE TABLE `t` (`id` INT) ENGINE = INNDB PARTITION BY RANGE (`id`) INTERVAL (MONTH, 1)  (PARTITION p20211101 VALUES LESS THAN ('2021-11-01 00:00:00'))"},

		{"CREATE TABLE t (id int) ENGINE = INNDB PARTITION BY RANGE COLUMNS (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20));", true, "CREATE TABLE `t` (`id` INT) ENGINE = INNDB PARTITION BY RANGE COLUMNS (`id`) (PARTITION `p0` VALUES LESS THAN (10),PARTITION `p1` VALUES LESS THAN (20))"},
		{"create table t (c int) PARTITION BY HASH (c) PARTITIONS 32;", true, "CREATE TABLE `t` (`c` INT) PARTITION BY HASH (`c`) PARTITIONS 32"},
		{"create table t (c int) PARTITION BY HASH (Year(VDate)) (PARTITION p1980 VALUES LESS THAN (1980) ENGINE = MyISAM, PARTITION p1990 VALUES LESS THAN (1990) ENGINE = MyISAM, PARTITION pothers VALUES LESS THAN MAXVALUE ENGINE = MyISAM)", false, ""},
		{"create table t (c int) PARTITION BY RANGE (Year(VDate)) (PARTITION p1980 VALUES LESS THAN (1980) ENGINE = MyISAM, PARTITION p1990 VALUES LESS THAN (1990) ENGINE = MyISAM, PARTITION pothers VALUES LESS THAN MAXVALUE ENGINE = MyISAM)", true, "CREATE TABLE `t` (`c` INT) PARTITION BY RANGE (YEAR(`VDate`)) (PARTITION `p1980` VALUES LESS THAN (1980) ENGINE = MyISAM,PARTITION `p1990` VALUES LESS THAN (1990) ENGINE = MyISAM,PARTITION `pothers` VALUES LESS THAN (MAXVALUE) ENGINE = MyISAM)"},
		{"create table t (c int, `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '') PARTITION BY RANGE (UNIX_TIMESTAMP(create_time)) (PARTITION p201610 VALUES LESS THAN(1477929600), PARTITION p201611 VALUES LESS THAN(1480521600),PARTITION p201612 VALUES LESS THAN(1483200000),PARTITION p201701 VALUES LESS THAN(1485878400),PARTITION p201702 VALUES LESS THAN(1488297600),PARTITION p201703 VALUES LESS THAN(1490976000))", true, "CREATE TABLE `t` (`c` INT,`create_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT '') PARTITION BY RANGE (UNIX_TIMESTAMP(`create_time`)) (PARTITION `p201610` VALUES LESS THAN (1477929600),PARTITION `p201611` VALUES LESS THAN (1480521600),PARTITION `p201612` VALUES LESS THAN (1483200000),PARTITION `p201701` VALUES LESS THAN (1485878400),PARTITION `p201702` VALUES LESS THAN (1488297600),PARTITION `p201703` VALUES LESS THAN (1490976000))"},
		{"CREATE TABLE `md_product_shop` (`shopCode` varchar(4) DEFAULT NULL COMMENT '地点') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!50100 PARTITION BY KEY (shopCode) PARTITIONS 19 */;", true, "CREATE TABLE `md_product_shop` (`shopCode` VARCHAR(4) DEFAULT NULL COMMENT '地点') ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 PARTITION BY KEY (`shopCode`) PARTITIONS 19"},
		{"CREATE TABLE `payinfo1` (`id` bigint(20) NOT NULL AUTO_INCREMENT, `oderTime` datetime NOT NULL) ENGINE=InnoDB AUTO_INCREMENT=641533032 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 /*!50500 PARTITION BY RANGE COLUMNS(oderTime) (PARTITION P2011 VALUES LESS THAN ('2012-01-01 00:00:00') ENGINE = InnoDB, PARTITION P1201 VALUES LESS THAN ('2012-02-01 00:00:00') ENGINE = InnoDB, PARTITION PMAX VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)*/;", true, "CREATE TABLE `payinfo1` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`oderTime` DATETIME NOT NULL) ENGINE = InnoDB AUTO_INCREMENT = 641533032 DEFAULT CHARACTER SET = UTF8 ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8 PARTITION BY RANGE COLUMNS (`oderTime`) (PARTITION `P2011` VALUES LESS THAN ('2012-01-01 00:00:00') ENGINE = InnoDB,PARTITION `P1201` VALUES LESS THAN ('2012-02-01 00:00:00') ENGINE = InnoDB,PARTITION `PMAX` VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)"},
		{`CREATE TABLE app_channel_daily_report (id bigint(20) NOT NULL AUTO_INCREMENT, app_version varchar(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'default', gmt_create datetime NOT NULL COMMENT '创建时间', PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=33703438 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
/*!50100 PARTITION BY RANGE (month(gmt_create)-1)
(PARTITION part0 VALUES LESS THAN (1) COMMENT = '1月份' ENGINE = InnoDB,
 PARTITION part1 VALUES LESS THAN (2) COMMENT = '2月份' ENGINE = InnoDB,
 PARTITION part2 VALUES LESS THAN (3) COMMENT = '3月份' ENGINE = InnoDB,
 PARTITION part3 VALUES LESS THAN (4) COMMENT = '4月份' ENGINE = InnoDB,
 PARTITION part4 VALUES LESS THAN (5) COMMENT = '5月份' ENGINE = InnoDB,
 PARTITION part5 VALUES LESS THAN (6) COMMENT = '6月份' ENGINE = InnoDB,
 PARTITION part6 VALUES LESS THAN (7) COMMENT = '7月份' ENGINE = InnoDB,
 PARTITION part7 VALUES LESS THAN (8) COMMENT = '8月份' ENGINE = InnoDB,
 PARTITION part8 VALUES LESS THAN (9) COMMENT = '9月份' ENGINE = InnoDB,
 PARTITION part9 VALUES LESS THAN (10) COMMENT = '10月份' ENGINE = InnoDB,
 PARTITION part10 VALUES LESS THAN (11) COMMENT = '11月份' ENGINE = InnoDB,
 PARTITION part11 VALUES LESS THAN (12) COMMENT = '12月份' ENGINE = InnoDB) */ ;`, true, "CREATE TABLE `app_channel_daily_report` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`app_version` VARCHAR(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'default',`gmt_create` DATETIME NOT NULL COMMENT '创建时间',PRIMARY KEY(`id`)) ENGINE = InnoDB AUTO_INCREMENT = 33703438 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_UNICODE_CI PARTITION BY RANGE (MONTH(`gmt_create`)-1) (PARTITION `part0` VALUES LESS THAN (1) COMMENT = '1月份' ENGINE = InnoDB,PARTITION `part1` VALUES LESS THAN (2) COMMENT = '2月份' ENGINE = InnoDB,PARTITION `part2` VALUES LESS THAN (3) COMMENT = '3月份' ENGINE = InnoDB,PARTITION `part3` VALUES LESS THAN (4) COMMENT = '4月份' ENGINE = InnoDB,PARTITION `part4` VALUES LESS THAN (5) COMMENT = '5月份' ENGINE = InnoDB,PARTITION `part5` VALUES LESS THAN (6) COMMENT = '6月份' ENGINE = InnoDB,PARTITION `part6` VALUES LESS THAN (7) COMMENT = '7月份' ENGINE = InnoDB,PARTITION `part7` VALUES LESS THAN (8) COMMENT = '8月份' ENGINE = InnoDB,PARTITION `part8` VALUES LESS THAN (9) COMMENT = '9月份' ENGINE = InnoDB,PARTITION `part9` VALUES LESS THAN (10) COMMENT = '10月份' ENGINE = InnoDB,PARTITION `part10` VALUES LESS THAN (11) COMMENT = '11月份' ENGINE = InnoDB,PARTITION `part11` VALUES LESS THAN (12) COMMENT = '12月份' ENGINE = InnoDB)"},

		// for check clause
		{"create table t (c1 bool, c2 bool, check (c1 in (0, 1)), check (c2 in (0, 1)))", true, "CREATE TABLE `t` (`c1` TINYINT(1),`c2` TINYINT(1))"},        //TODO: Check in ColumnOption, yacc is not implemented
		{"CREATE TABLE Customer (SD integer CHECK (SD > 0), First_Name varchar(30));", true, "CREATE TABLE `Customer` (`SD` INT ,`First_Name` VARCHAR(30))"}, //TODO: Check in ColumnOption, yacc is not implemented

		{"create database xxx", true, "CREATE DATABASE `xxx`"},
		{"create database if exists xxx", false, ""},
		{"create database if not exists xxx", true, "CREATE DATABASE IF NOT EXISTS `xxx`"},
		{"create schema xxx", true, "CREATE DATABASE `xxx`"},
		{"create schema if exists xxx", false, ""},
		{"create schema if not exists xxx", true, "CREATE DATABASE IF NOT EXISTS `xxx`"},
		// for drop database/schema/table/view/stats
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"drop database if exists xxx", true, "DROP DATABASE IF EXISTS `xxx`"},
		{"drop database if not exists xxx", false, ""},
		{"drop schema xxx", true, "DROP DATABASE `xxx`"},
		{"drop schema if exists xxx", true, "DROP DATABASE IF EXISTS `xxx`"},
		{"drop schema if not exists xxx", false, ""},
		{"drop table", false, "DROP TABLE"},
		{"drop table xxx", true, "DROP TABLE `xxx`"},
		{"drop table xxx, yyy", true, "DROP TABLE `xxx`, `yyy`"},
		{"drop tables xxx", true, "DROP TABLE `xxx`"},
		{"drop tables xxx, yyy", true, "DROP TABLE `xxx`, `yyy`"},
		{"drop table if exists xxx", true, "DROP TABLE IF EXISTS `xxx`"},
		{"drop table if exists xxx, yyy", true, "DROP TABLE IF EXISTS `xxx`, `yyy`"},
		{"drop table if not exists xxx", false, ""},
		{"drop table xxx restrict", true, "DROP TABLE `xxx`"},
		{"drop table xxx, yyy cascade", true, "DROP TABLE `xxx`, `yyy`"},
		{"drop table if exists xxx restrict", true, "DROP TABLE IF EXISTS `xxx`"},
		// for drop partition table
		{"drop PARTITION  table xxx", true, "DROP PARTITION TABLE `xxx`"},
		{"drop view", false, "DROP VIEW"},
		{"drop view xxx", true, "DROP VIEW `xxx`"},
		{"drop view xxx, yyy", true, "DROP VIEW `xxx`, `yyy`"},
		{"drop view if exists xxx", true, "DROP VIEW IF EXISTS `xxx`"},
		{"drop view if exists xxx, yyy", true, "DROP VIEW IF EXISTS `xxx`, `yyy`"},
		{"drop stats t", true, "DROP STATS `t`"},
		// for issue 974
		{`CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(128) NOT NULL,
		address_detail varchar(128) NOT NULL,
		cellphone varchar(16) NOT NULL,
		latitude double NOT NULL,
		longitude double NOT NULL,
		name varchar(16) NOT NULL,
		sex tinyint(1) NOT NULL,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET UTF8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`),INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		// for issue 975
		{`CREATE TABLE test_data (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(255) NOT NULL,
		amount decimal(19,2) DEFAULT NULL,
		charge_id varchar(32) DEFAULT NULL,
		paid_amount decimal(19,2) DEFAULT NULL,
		transaction_no varchar(64) DEFAULT NULL,
		wx_mp_app_id varchar(32) DEFAULT NULL,
		contacts varchar(50) DEFAULT NULL,
		deliver_fee decimal(19,2) DEFAULT NULL,
		deliver_info varchar(255) DEFAULT NULL,
		deliver_time varchar(255) DEFAULT NULL,
		description varchar(255) DEFAULT NULL,
		invoice varchar(255) DEFAULT NULL,
		order_from int(11) DEFAULT NULL,
		order_state int(11) NOT NULL,
		packing_fee decimal(19,2) DEFAULT NULL,
		payment_time datetime DEFAULT NULL,
		payment_type int(11) DEFAULT NULL,
		phone varchar(50) NOT NULL,
		store_employee_id bigint(20) DEFAULT NULL,
		store_id bigint(20) NOT NULL,
		user_id bigint(20) NOT NULL,
		payment_mode int(11) NOT NULL,
		current_latitude double NOT NULL,
		current_longitude double NOT NULL,
		address_latitude double NOT NULL,
		address_longitude double NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT food_order_ibfk_1 FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		CONSTRAINT food_order_ibfk_2 FOREIGN KEY (store_id) REFERENCES waimaiqa.store (id),
		CONSTRAINT food_order_ibfk_3 FOREIGN KEY (store_employee_id) REFERENCES waimaiqa.store_employee (id),
		UNIQUE FK_UNIQUE_charge_id USING BTREE (charge_id) comment '',
		INDEX FK_eqst2x1xisn3o0wbrlahnnqq8 USING BTREE (store_employee_id) comment '',
		INDEX FK_8jcmec4kb03f4dod0uqwm54o9 USING BTREE (store_id) comment '',
		INDEX FK_a3t0m9apja9jmrn60uab30pqd USING BTREE (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=95 DEFAULT CHARACTER SET utf8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `test_data` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(255) NOT NULL,`amount` DECIMAL(19,2) DEFAULT NULL,`charge_id` VARCHAR(32) DEFAULT NULL,`paid_amount` DECIMAL(19,2) DEFAULT NULL,`transaction_no` VARCHAR(64) DEFAULT NULL,`wx_mp_app_id` VARCHAR(32) DEFAULT NULL,`contacts` VARCHAR(50) DEFAULT NULL,`deliver_fee` DECIMAL(19,2) DEFAULT NULL,`deliver_info` VARCHAR(255) DEFAULT NULL,`deliver_time` VARCHAR(255) DEFAULT NULL,`description` VARCHAR(255) DEFAULT NULL,`invoice` VARCHAR(255) DEFAULT NULL,`order_from` INT(11) DEFAULT NULL,`order_state` INT(11) NOT NULL,`packing_fee` DECIMAL(19,2) DEFAULT NULL,`payment_time` DATETIME DEFAULT NULL,`payment_type` INT(11) DEFAULT NULL,`phone` VARCHAR(50) NOT NULL,`store_employee_id` BIGINT(20) DEFAULT NULL,`store_id` BIGINT(20) NOT NULL,`user_id` BIGINT(20) NOT NULL,`payment_mode` INT(11) NOT NULL,`current_latitude` DOUBLE NOT NULL,`current_longitude` DOUBLE NOT NULL,`address_latitude` DOUBLE NOT NULL,`address_longitude` DOUBLE NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `food_order_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`),CONSTRAINT `food_order_ibfk_2` FOREIGN KEY (`store_id`) REFERENCES `waimaiqa`.`store`(`id`),CONSTRAINT `food_order_ibfk_3` FOREIGN KEY (`store_employee_id`) REFERENCES `waimaiqa`.`store_employee`(`id`),UNIQUE `FK_UNIQUE_charge_id`(`charge_id`) USING BTREE,INDEX `FK_eqst2x1xisn3o0wbrlahnnqq8`(`store_employee_id`) USING BTREE,INDEX `FK_8jcmec4kb03f4dod0uqwm54o9`(`store_id`) USING BTREE,INDEX `FK_a3t0m9apja9jmrn60uab30pqd`(`user_id`) USING BTREE) ENGINE = InnoDB AUTO_INCREMENT = 95 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		{`create table t (c int KEY);`, true, "CREATE TABLE `t` (`c` INT PRIMARY KEY)"},
		{`CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(128) NOT NULL,
		address_detail varchar(128) NOT NULL,
		cellphone varchar(16) NOT NULL,
		latitude double NOT NULL,
		longitude double NOT NULL,
		name varchar(16) NOT NULL,
		sex tinyint(1) NOT NULL,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id) ON DELETE CASCADE ON UPDATE NO ACTION,
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE UTF8_GENERAL_CI ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`) ON DELETE CASCADE ON UPDATE NO ACTION,INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		{"CREATE TABLE address (\r\nid bigint(20) NOT NULL AUTO_INCREMENT,\r\ncreate_at datetime NOT NULL,\r\ndeleted tinyint(1) NOT NULL,\r\nupdate_at datetime NOT NULL,\r\nversion bigint(20) DEFAULT NULL,\r\naddress varchar(128) NOT NULL,\r\naddress_detail varchar(128) NOT NULL,\r\ncellphone varchar(16) NOT NULL,\r\nlatitude double NOT NULL,\r\nlongitude double NOT NULL,\r\nname varchar(16) NOT NULL,\r\nsex tinyint(1) NOT NULL,\r\nuser_id bigint(20) NOT NULL,\r\nPRIMARY KEY (id),\r\nCONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id) ON DELETE CASCADE ON UPDATE NO ACTION,\r\nINDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''\r\n) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;", true, "CREATE TABLE `address` (`id` BIGINT(20) NOT NULL AUTO_INCREMENT,`create_at` DATETIME NOT NULL,`deleted` TINYINT(1) NOT NULL,`update_at` DATETIME NOT NULL,`version` BIGINT(20) DEFAULT NULL,`address` VARCHAR(128) NOT NULL,`address_detail` VARCHAR(128) NOT NULL,`cellphone` VARCHAR(16) NOT NULL,`latitude` DOUBLE NOT NULL,`longitude` DOUBLE NOT NULL,`name` VARCHAR(16) NOT NULL,`sex` TINYINT(1) NOT NULL,`user_id` BIGINT(20) NOT NULL,PRIMARY KEY(`id`),CONSTRAINT `FK_7rod8a71yep5vxasb0ms3osbg` FOREIGN KEY (`user_id`) REFERENCES `waimaiqa`.`user`(`id`) ON DELETE CASCADE ON UPDATE NO ACTION,INDEX `FK_7rod8a71yep5vxasb0ms3osbg`(`user_id`) ) ENGINE = InnoDB AUTO_INCREMENT = 30 DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI ROW_FORMAT = COMPACT COMMENT = '' CHECKSUM = 0 DELAY_KEY_WRITE = 0"},
		// for issue 1802
		{`CREATE TABLE t1 (
		accout_id int(11) DEFAULT '0',
		summoner_id int(11) DEFAULT '0',
		union_name varbinary(52) NOT NULL,
		union_id int(11) DEFAULT '0',
		PRIMARY KEY (union_name)) ENGINE=MyISAM DEFAULT CHARSET=binary;`, true, "CREATE TABLE `t1` (`accout_id` INT(11) DEFAULT '0',`summoner_id` INT(11) DEFAULT '0',`union_name` VARBINARY(52) NOT NULL,`union_id` INT(11) DEFAULT '0',PRIMARY KEY(`union_name`)) ENGINE = MyISAM DEFAULT CHARACTER SET = BINARY"},
		// for issue pingcap/parser#310
		{`CREATE TABLE t (a DECIMAL(20,0), b DECIMAL(30), c FLOAT(25,0))`, true, "CREATE TABLE `t` (`a` DECIMAL(20,0),`b` DECIMAL(30),`c` FLOAT(25,0))"},
		// Create table with multiple index options.
		{`create table t (c int, index ci (c) USING BTREE COMMENT "123");`, true, "CREATE TABLE `t` (`c` INT,INDEX `ci`(`c`) USING BTREE COMMENT '123')"},
		// for default value
		{"CREATE TABLE sbtest (id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, k integer UNSIGNED DEFAULT '0' NOT NULL, c char(120) DEFAULT '' NOT NULL, pad char(60) DEFAULT '' NOT NULL, PRIMARY KEY  (id) )", true, "CREATE TABLE `sbtest` (`id` INT UNSIGNED NOT NULL AUTO_INCREMENT,`k` INT UNSIGNED DEFAULT '0' NOT NULL,`c` CHAR(120) DEFAULT '' NOT NULL,`pad` CHAR(60) DEFAULT '' NOT NULL,PRIMARY KEY(`id`))"},
		{"create table test (create_date TIMESTAMP NOT NULL COMMENT '创建日期 create date' DEFAULT now());", true, "CREATE TABLE `test` (`create_date` TIMESTAMP NOT NULL COMMENT '创建日期 create date' DEFAULT CURRENT_TIMESTAMP())"},
		{"create table ts (t int, v timestamp(3) default CURRENT_TIMESTAMP(3));", true, "CREATE TABLE `ts` (`t` INT,`v` TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"}, //TODO: The number yacc in parentheses has not been implemented yet.
		// Create table with primary key name.
		{"create table if not exists `t` (`id` int not null auto_increment comment '消息ID', primary key `pk_id` (`id`) );", true, "CREATE TABLE IF NOT EXISTS `t` (`id` INT NOT NULL AUTO_INCREMENT COMMENT '消息ID',PRIMARY KEY `pk_id`(`id`))"},
		// Create table with like.
		{"create table a like b", true, "CREATE TABLE `a` LIKE `b`"},
		{"create table a (id int REFERENCES a (id) ON delete NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE NO ACTION)"},
		{"create table a (id int REFERENCES a (id) ON update set default )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE SET DEFAULT)"},
		{"create table a (id int REFERENCES a (id) ON delete set null on update CASCADE)", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE SET NULL ON UPDATE CASCADE)"},
		{"create table a (id int REFERENCES a (id) ON update set default on delete RESTRICT)", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON DELETE RESTRICT ON UPDATE SET DEFAULT)"},
		{"create table a (id int REFERENCES a (id) MATCH FULL ON delete NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH FULL ON DELETE NO ACTION)"},
		{"create table a (id int REFERENCES a (id) MATCH PARTIAL ON update NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH PARTIAL ON UPDATE NO ACTION)"},
		{"create table a (id int REFERENCES a (id) MATCH SIMPLE ON update NO ACTION )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) MATCH SIMPLE ON UPDATE NO ACTION)"},
		{"create table a (id int REFERENCES a (id) ON update set default )", true, "CREATE TABLE `a` (`id` INT REFERENCES `a`(`id`) ON UPDATE SET DEFAULT)"},
		{"create table a (id int REFERENCES a (id) ON update set default on update CURRENT_TIMESTAMP)", false, ""},
		{"create table a (id int REFERENCES a (id) ON delete set default on update CURRENT_TIMESTAMP)", false, ""},
		{"create table a (like b)", true, "CREATE TABLE `a` LIKE `b`"},
		{"create table if not exists a like b", true, "CREATE TABLE IF NOT EXISTS `a` LIKE `b`"},
		{"create table if not exists a (like b)", true, "CREATE TABLE IF NOT EXISTS `a` LIKE `b`"},
		{"create table if not exists a like (b)", false, ""},
		{"create table a (t int) like b", false, ""},
		{"create table a (t int) like (b)", false, ""},
		// Create table with select statement
		{"create table a select * from b", true, "CREATE TABLE `a` AS SELECT * FROM `b`"},
		{"create table a as select * from b", true, "CREATE TABLE `a` AS SELECT * FROM `b`"},
		{"create table a (m int, n datetime) as select * from b", true, "CREATE TABLE `a` (`m` INT,`n` DATETIME) AS SELECT * FROM `b`"},
		{"create table a (unique(n)) as select n from b", true, "CREATE TABLE `a` (UNIQUE(`n`)) AS SELECT `n` FROM `b`"},
		{"create table a ignore as select n from b", true, "CREATE TABLE `a` IGNORE AS SELECT `n` FROM `b`"},
		{"create table a replace as select n from b", true, "CREATE TABLE `a` REPLACE AS SELECT `n` FROM `b`"},
		{"create table a (m int) replace as (select n as m from b union select n+1 as m from c group by 1 limit 2)", true, "CREATE TABLE `a` (`m` INT) REPLACE AS (SELECT `n` AS `m` FROM `b` UNION SELECT `n`+1 AS `m` FROM `c` GROUP BY 1 LIMIT 2)"},

		// Create table with no option is valid for parser
		{"create table a", true, "CREATE TABLE `a`"},

		{"create table t (a timestamp default now)", false, ""},
		{"create table t (a timestamp default now())", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())"},
		{"create table t (a timestamp default (((now()))))", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP())"},
		{"create table t (a timestamp default now() on update now)", false, ""},
		{"create table t (a timestamp default now() on update now())", true, "CREATE TABLE `t` (`a` TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP())"},
		{"create table t (a timestamp default now() on update (now()))", false, ""},
		{"CREATE TABLE t (c TEXT) default CHARACTER SET utf8, default COLLATE utf8_general_ci;", true, "CREATE TABLE `t` (`c` TEXT) DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = UTF8_GENERAL_CI"},
		{"CREATE TABLE t (c TEXT) shard_row_id_bits = 1;", true, "CREATE TABLE `t` (`c` TEXT) SHARD_ROW_ID_BITS = 1"},
		// {"CREATE TABLE t (c TEXT) shard_row_id_bits = 1, PRE_SPLIT_REGIONS = 1;", true, "CREATE TABLE `t` (`c` TEXT) SHARD_ROW_ID_BITS = 1 PRE_SPLIT_REGIONS = 1"},
		// Create table with ON UPDATE CURRENT_TIMESTAMP(6), specify fraction part.
		{"CREATE TABLE IF NOT EXISTS `general_log` (`event_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` mediumtext NOT NULL,`thread_id` bigint(20) unsigned NOT NULL,`server_id` int(10) unsigned NOT NULL,`command_type` varchar(64) NOT NULL,`argument` mediumblob NOT NULL) ENGINE=CSV DEFAULT CHARSET=utf8 COMMENT='General log'", true, "CREATE TABLE IF NOT EXISTS `general_log` (`event_time` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` MEDIUMTEXT NOT NULL,`thread_id` BIGINT(20) UNSIGNED NOT NULL,`server_id` INT(10) UNSIGNED NOT NULL,`command_type` VARCHAR(64) NOT NULL,`argument` MEDIUMBLOB NOT NULL) ENGINE = CSV DEFAULT CHARACTER SET = UTF8 COMMENT = 'General log'"}, //TODO: The number yacc in parentheses has not been implemented yet.
		// For reference_definition in column_definition.
		{"CREATE TABLE followers ( f1 int NOT NULL REFERENCES user_profiles (uid) );", true, "CREATE TABLE `followers` (`f1` INT NOT NULL REFERENCES `user_profiles`(`uid`))"},

		// For column default expression
		{"create table t (a int default rand())", true, "CREATE TABLE `t` (`a` INT DEFAULT (RAND()))"},
		{"create table t (a int default rand(1))", true, "CREATE TABLE `t` (`a` INT DEFAULT (RAND(1)))"},
		{"create table t (a int default (rand()))", true, "CREATE TABLE `t` (`a` INT DEFAULT (RAND()))"},
		{"create table t (a int default (rand(1)))", true, "CREATE TABLE `t` (`a` INT DEFAULT (RAND(1)))"},
		{"create table t (a int default (((rand()))))", true, "CREATE TABLE `t` (`a` INT DEFAULT (RAND()))"},
		{"create table t (a int default (((rand(1)))))", true, "CREATE TABLE `t` (`a` INT DEFAULT (RAND(1)))"},
		{"create table t (d date default current_date())", true, "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))"},
		{"create table t (d date default current_date)", true, "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))"},
		{"create table t (d date default (current_date()))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))"},
		{"create table t (d date default (curdate()))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))"},
		{"create table t (d date default curdate())", true, "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))"},
		{"create table t (d date default current_date())", true, "CREATE TABLE `t` (`d` DATE DEFAULT (CURRENT_DATE()))"},
		{"create table t (d date default date_format(now(),'%Y-%m'))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (DATE_FORMAT(NOW(), _UTF8MB4'%Y-%m')))"},
		{"create table t (d date default (date_format(now(),'%Y-%m')))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (DATE_FORMAT(NOW(), _UTF8MB4'%Y-%m')))"},
		{"create table t (d date default date_format(now(),'%Y-%m-%d'))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (DATE_FORMAT(NOW(), _UTF8MB4'%Y-%m-%d')))"},
		{"create table t (d date default date_format(now(),'%Y-%m-%d %H.%i.%s'))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (DATE_FORMAT(NOW(), _UTF8MB4'%Y-%m-%d %H.%i.%s')))"},
		{"create table t (d date default date_format(now(),'%Y-%m-%d %H:%i:%s'))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (DATE_FORMAT(NOW(), _UTF8MB4'%Y-%m-%d %H:%i:%s')))"},
		{"create table t (d date default date_format(now(),'%b %d %Y %h:%i %p'))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (DATE_FORMAT(NOW(), _UTF8MB4'%b %d %Y %h:%i %p')))"},
		{"create table t (a varchar(32) default (replace(upper(uuid()), '-', '')))", true, "CREATE TABLE `t` (`a` VARCHAR(32) DEFAULT (REPLACE(UPPER(UUID()), _UTF8MB4'-', _UTF8MB4'')))"},
		{"create table t (a varchar(32) default replace(upper(uuid()), '-', ''))", true, "CREATE TABLE `t` (`a` VARCHAR(32) DEFAULT (REPLACE(UPPER(UUID()), _UTF8MB4'-', _UTF8MB4'')))"},
		{"create table t (a varchar(32) default (replace(convert(upper(uuid()) using utf8mb4), '-', '')))", true, "CREATE TABLE `t` (`a` VARCHAR(32) DEFAULT (REPLACE(CONVERT(UPPER(UUID()) USING 'utf8mb4'), _UTF8MB4'-', _UTF8MB4'')))"},
		{"create table t (a varchar(32) default replace(convert(upper(uuid()) using utf8mb4), '-', ''))", true, "CREATE TABLE `t` (`a` VARCHAR(32) DEFAULT (REPLACE(CONVERT(UPPER(UUID()) USING 'utf8mb4'), _UTF8MB4'-', _UTF8MB4'')))"},
		{"create table t (a int default upper(substring_index(user(),'@',1)))", true, "CREATE TABLE `t` (`a` INT DEFAULT (UPPER(SUBSTRING_INDEX(USER(), _UTF8MB4'@', 1))))"},
		{"create table t (a int default (upper(substring_index(user(),'@',1))))", true, "CREATE TABLE `t` (`a` INT DEFAULT (UPPER(SUBSTRING_INDEX(USER(), _UTF8MB4'@', 1))))"},
		{"create table t (a varchar(32) default (str_to_date('1980-01-01','%Y-%m-%d')))", true, "CREATE TABLE `t` (`a` VARCHAR(32) DEFAULT (STR_TO_DATE(_UTF8MB4'1980-01-01', _UTF8MB4'%Y-%m-%d')))"},
		{"create table t (a varchar(32) default str_to_date('1980-01-01','%Y-%m-%d'))", true, "CREATE TABLE `t` (`a` VARCHAR(32) DEFAULT (STR_TO_DATE(_UTF8MB4'1980-01-01', _UTF8MB4'%Y-%m-%d')))"},
		{"create table t (d date default (utc_timestamp))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (UTC_TIMESTAMP))"},
		{"create table t (d date default (utc_timestamp()))", true, "CREATE TABLE `t` (`d` DATE DEFAULT (UTC_TIMESTAMP()))"},
		// create table with Default value expression column
		{"create table t (a varchar(32) default (a))", true, "CREATE TABLE `t` (`a` VARCHAR(32) DEFAULT (A))"},
		// create table with expression index
		{"create table a(a int, key(lower(a)));", false, ""},
		{"create table a(a int, key(a+1));", false, ""},
		{"create table a(a int, key(a, a+1));", false, ""},
		{"create table a(a int, b int, key((a+1), (b+1)));", true, "CREATE TABLE `a` (`a` INT,`b` INT,INDEX((`a`+1), (`b`+1)))"},
		{"create table a(a int, b int, key(a, (b+1)));", true, "CREATE TABLE `a` (`a` INT,`b` INT,INDEX(`a`, (`b`+1)))"},
		{"create table a(a int, b int, key((a+1), b));", true, "CREATE TABLE `a` (`a` INT,`b` INT,INDEX((`a`+1), `b`))"},
		{"create table a(a int, b int, key((a + 1) desc));", true, "CREATE TABLE `a` (`a` INT,`b` INT,INDEX((`a`+1)))"},
		// for alter database/schema/table
		// {"ALTER DATABASE t CHARACTER SET = 'utf8'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},
		// {"ALTER DATABASE CHARACTER SET = 'utf8'", true, "ALTER DATABASE CHARACTER SET = utf8"},
		// {"ALTER DATABASE t DEFAULT CHARACTER SET = 'utf8'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},
		// {"ALTER SCHEMA t DEFAULT CHARACTER SET = 'utf8'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},
		// {"ALTER SCHEMA DEFAULT CHARACTER SET = 'utf8'", true, "ALTER DATABASE CHARACTER SET = utf8"},
		// {"ALTER SCHEMA t DEFAULT CHARSET = 'UTF8'", true, "ALTER DATABASE `t` CHARACTER SET = utf8"},

		// {"ALTER DATABASE t COLLATE = 'utf8_bin'", true, "ALTER DATABASE `t` COLLATE = utf8_bin"},
		// {"ALTER DATABASE COLLATE = 'utf8_bin'", true, "ALTER DATABASE COLLATE = utf8_bin"},
		// {"ALTER DATABASE t DEFAULT COLLATE = 'utf8_bin'", true, "ALTER DATABASE `t` COLLATE = utf8_bin"},
		// {"ALTER SCHEMA t DEFAULT COLLATE = 'UTF8_BiN'", true, "ALTER DATABASE `t` COLLATE = utf8_bin"},
		// {"ALTER SCHEMA DEFAULT COLLATE = 'UTF8_BiN'", true, "ALTER DATABASE COLLATE = utf8_bin"},
		// {"ALTER SCHEMA `` DEFAULT COLLATE = 'UTF8_BiN'", true, "ALTER DATABASE `` COLLATE = utf8_bin"},

		// {"ALTER DATABASE t CHARSET = 'utf8mb4' COLLATE = 'utf8_bin'", true, "ALTER DATABASE `t` CHARACTER SET = utf8mb4 COLLATE = utf8_bin"},
		// {
		// 	"ALTER DATABASE t DEFAULT CHARSET = 'utf8mb4' DEFAULT COLLATE = 'utf8mb4_general_ci' CHARACTER SET = 'utf8' COLLATE = 'utf8mb4_bin'",
		// 	true,
		// 	"ALTER DATABASE `t` CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci CHARACTER SET = utf8 COLLATE = utf8mb4_bin",
		// },
		// {"ALTER DATABASE DEFAULT CHARSET = 'utf8mb4' COLLATE = 'utf8_bin'", true, "ALTER DATABASE CHARACTER SET = utf8mb4 COLLATE = utf8_bin"},
		// {
		// 	"ALTER DATABASE DEFAULT CHARSET = 'utf8mb4' DEFAULT COLLATE = 'utf8mb4_general_ci' CHARACTER SET = 'utf8' COLLATE = 'utf8mb4_bin'",
		// 	true,
		// 	"ALTER DATABASE CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci CHARACTER SET = utf8 COLLATE = utf8mb4_bin",
		// },
		// for references without IndexColNameList
		{"ALTER TABLE t.* ADD COLUMN (a SMALLINT UNSIGNED)", false, ""},
		{"alter table t add column a double (4,2) zerofill references b match full on update set null first", true, "ALTER TABLE `t` ADD COLUMN `a` DOUBLE(4,2) UNSIGNED ZEROFILL REFERENCES `b` MATCH FULL ON UPDATE SET NULL FIRST"},
		{"alter table d_n.t_n add constraint foreign key ident (ident(1)) references d_n.t_n match full on delete set null", true, "ALTER TABLE `d_n`.`t_n` ADD CONSTRAINT `ident` FOREIGN KEY (`ident`(1)) REFERENCES `d_n`.`t_n` MATCH FULL ON DELETE SET NULL"},
		{"alter table t_n add constraint ident foreign key (ident,ident(1)) references t_n match full on update set null on delete restrict", true, "ALTER TABLE `t_n` ADD CONSTRAINT `ident` FOREIGN KEY (`ident`, `ident`(1)) REFERENCES `t_n` MATCH FULL ON DELETE RESTRICT ON UPDATE SET NULL"},
		{"alter table d_n.t_n add foreign key ident (ident, ident(1) asc) references t_n match partial on delete cascade remove partitioning", true, "ALTER TABLE `d_n`.`t_n` ADD CONSTRAINT `ident` FOREIGN KEY (`ident`, `ident`(1)) REFERENCES `t_n` MATCH PARTIAL ON DELETE CASCADE REMOVE PARTITIONING"},
		{"alter table d_n.t_n add constraint foreign key (ident asc) references d_n.t_n match simple on update cascade on delete cascade", true, "ALTER TABLE `d_n`.`t_n` ADD CONSTRAINT FOREIGN KEY (`ident`) REFERENCES `d_n`.`t_n` MATCH SIMPLE ON DELETE CASCADE ON UPDATE CASCADE"},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED)", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED)"},
		{"ALTER TABLE ADD COLUMN (a SMALLINT UNSIGNED)", false, ""},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED, b varchar(255))", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED FIRST)", false, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED FIRST", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED FIRST"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED AFTER b", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED AFTER `b`"},
		{"ALTER TABLE employees ADD PARTITION", true, "ALTER TABLE `employees` ADD PARTITION"},
		{"ALTER TABLE employees ADD PARTITION ( PARTITION P1 VALUES LESS THAN (2010))", true, "ALTER TABLE `employees` ADD PARTITION (PARTITION `P1` VALUES LESS THAN (2010))"},
		{"ALTER TABLE employees ADD PARTITION ( PARTITION P2 VALUES LESS THAN MAXVALUE)", true, "ALTER TABLE `employees` ADD PARTITION (PARTITION `P2` VALUES LESS THAN (MAXVALUE))"},
		{`ALTER TABLE employees ADD PARTITION (
				PARTITION P1 VALUES LESS THAN (2010),
				PARTITION P2 VALUES LESS THAN (2015),
				PARTITION P3 VALUES LESS THAN MAXVALUE)`, true, "ALTER TABLE `employees` ADD PARTITION (PARTITION `P1` VALUES LESS THAN (2010), PARTITION `P2` VALUES LESS THAN (2015), PARTITION `P3` VALUES LESS THAN (MAXVALUE))"},
		{"alter table t add partition (partition x values in ((3, 4), (5, 6)))", true, "ALTER TABLE `t` ADD PARTITION (PARTITION `x` VALUES IN ((3, 4), (5, 6)))"},
		// column visibility option
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED visible", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED VISIBLE"},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED, b varchar(255) visible)", true, "ALTER TABLE `t` ADD COLUMN (`a` SMALLINT UNSIGNED, `b` VARCHAR(255) VISIBLE)"},
		{"ALTER TABLE t MODIFY COLUMN a varchar(255) visible", true, "ALTER TABLE `t` MODIFY COLUMN `a` VARCHAR(255) VISIBLE"},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255) visible", true, "ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255) VISIBLE"},
		{"ALTER TABLE t ALTER COLUMN a SET visible", true, "ALTER TABLE `t` ALTER COLUMN `a` SET VISIBLE"},
		// For drop table partition statement.
		{"alter table t first partition less than (1);", true, "ALTER TABLE `t` FIRST PARTITION LESS THAN (1)"},
		{"alter table t last partition less than (1);", true, "ALTER TABLE `t` LAST PARTITION LESS THAN (1)"},
		{"alter table t drop partition p1;", true, "ALTER TABLE `t` DROP PARTITION `p1`"},
		{"alter table t drop partition p2;", true, "ALTER TABLE `t` DROP PARTITION `p2`"},
		// {"alter table t drop partition p1, p2;", true, "ALTER TABLE `t` DROP PARTITION `p1`,`p2`"},
		// {"alter table employees add partition partitions 1;", true, "ALTER TABLE `employees` ADD PARTITION PARTITIONS 1"},
		// {"alter table employees add partition partitions 2;", true, "ALTER TABLE `employees` ADD PARTITION PARTITIONS 2"},
		// {"alter table clients coalesce partition 3;", true, "ALTER TABLE `clients` COALESCE PARTITION 3"},
		// {"alter table clients coalesce partition 4;", true, "ALTER TABLE `clients` COALESCE PARTITION 4"},
		{"ALTER TABLE t DISABLE KEYS", true, "ALTER TABLE `t` DISABLE KEYS"},
		{"ALTER TABLE t ENABLE KEYS", true, "ALTER TABLE `t` ENABLE KEYS"},
		{"ALTER TABLE t MODIFY COLUMN a varchar(255)", true, "ALTER TABLE `t` MODIFY COLUMN `a` VARCHAR(255)"},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255)", true, "ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255)"},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255) CHARACTER SET UTF8 BINARY", true, "ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255) BINARY CHARACTER SET UTF8"},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255) FIRST", true, "ALTER TABLE `t` CHANGE COLUMN `a` `b` VARCHAR(255) FIRST"},
		{"ALTER TABLE db.t RENAME to db1.t1", true, "ALTER TABLE `db`.`t` RENAME AS `db1`.`t1`"},
		{"ALTER TABLE db.t RENAME db1.t1", true, "ALTER TABLE `db`.`t` RENAME AS `db1`.`t1`"},
		{"ALTER TABLE t RENAME as t1", true, "ALTER TABLE `t` RENAME AS `t1`"},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT 1", true, "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1"},
		{"ALTER TABLE t ALTER a SET DEFAULT 1", true, "ALTER TABLE `t` ALTER COLUMN `a` SET DEFAULT 1"},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT CURRENT_TIMESTAMP", false, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT NOW()", false, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT 1+1", false, ""},
		{"ALTER TABLE t ALTER COLUMN a DROP DEFAULT", true, "ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT"},
		{"ALTER TABLE t ALTER a DROP DEFAULT", true, "ALTER TABLE `t` ALTER COLUMN `a` DROP DEFAULT"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=none", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = NONE"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=default", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = DEFAULT"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=shared", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = SHARED"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=exclusive", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = EXCLUSIVE"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=NONE", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = NONE"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=DEFAULT", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = DEFAULT"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=SHARED", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = SHARED"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=EXCLUSIVE", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, LOCK = EXCLUSIVE"},
		{"ALTER TABLE t ADD FULLTEXT KEY `FullText` (`name` ASC)", true, "ALTER TABLE `t` ADD FULLTEXT `FullText`(`name`)"},
		{"ALTER TABLE t ADD FULLTEXT `FullText` (`name` ASC)", true, "ALTER TABLE `t` ADD FULLTEXT `FullText`(`name`)"},
		{"ALTER TABLE t ADD FULLTEXT INDEX `FullText` (`name` ASC)", true, "ALTER TABLE `t` ADD FULLTEXT `FullText`(`name`)"},
		{"ALTER TABLE t ADD INDEX (a) USING BTREE COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX(`a`) USING BTREE COMMENT 'a'"},
		{"ALTER TABLE t ADD KEY (a) USING HASH COMMENT 'a'", true, "ALTER TABLE `t` ADD INDEX(`a`) USING HASH COMMENT 'a'"},
		{"ALTER TABLE t ADD PRIMARY KEY (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD PRIMARY KEY(`a`) COMMENT 'a'"},
		{"ALTER TABLE t ADD UNIQUE (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"},
		{"ALTER TABLE t ADD UNIQUE KEY (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"},
		{"ALTER TABLE t ADD UNIQUE INDEX (a) COMMENT 'a'", true, "ALTER TABLE `t` ADD UNIQUE(`a`) COMMENT 'a'"},
		{"ALTER TABLE t ADD PRIMARY KEY ident USING RTREE ( a DESC , b   )", true, "ALTER TABLE `t` ADD PRIMARY KEY `ident`(`a`, `b`) USING RTREE"},

		{"ALTER TABLE t ENGINE ''", true, "ALTER TABLE `t` ENGINE = ''"},
		{"ALTER TABLE t ENGINE = ''", true, "ALTER TABLE `t` ENGINE = ''"},
		{"ALTER TABLE t ENGINE = 'innodb'", true, "ALTER TABLE `t` ENGINE = innodb"},
		{"ALTER TABLE t ENGINE = innodb", true, "ALTER TABLE `t` ENGINE = innodb"},
		{"ALTER TABLE `db`.`t` ENGINE = ``", true, "ALTER TABLE `db`.`t` ENGINE = ''"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, ADD COLUMN a SMALLINT", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT UNSIGNED, ADD COLUMN `a` SMALLINT"},
		{"ALTER TABLE t ADD COLUMN a SMALLINT, ENGINE = '', default COLLATE = UTF8_GENERAL_CI", true, "ALTER TABLE `t` ADD COLUMN `a` SMALLINT, ENGINE = '', DEFAULT COLLATE = UTF8_GENERAL_CI"},
		{"ALTER TABLE t ENGINE = '', COMMENT='', default COLLATE = UTF8_GENERAL_CI", true, "ALTER TABLE `t` ENGINE = '', COMMENT = '', DEFAULT COLLATE = UTF8_GENERAL_CI"},
		{"ALTER TABLE t ENGINE = '', ADD COLUMN a SMALLINT", true, "ALTER TABLE `t` ENGINE = '', ADD COLUMN `a` SMALLINT"},
		{"ALTER TABLE t default COLLATE = UTF8_GENERAL_CI, ENGINE = '', ADD COLUMN a SMALLINT", true, "ALTER TABLE `t` DEFAULT COLLATE = UTF8_GENERAL_CI, ENGINE = '', ADD COLUMN `a` SMALLINT"},
		{"ALTER TABLE t shard_row_id_bits = 1", true, "ALTER TABLE `t` SHARD_ROW_ID_BITS = 1"},
		{"ALTER TABLE t AUTO_INCREMENT 3", true, "ALTER TABLE `t` AUTO_INCREMENT = 3"},
		{"ALTER TABLE t AUTO_INCREMENT = 3", true, "ALTER TABLE `t` AUTO_INCREMENT = 3"},
		{"ALTER TABLE t FORCE AUTO_INCREMENT 3", true, "ALTER TABLE `t` FORCE AUTO_INCREMENT = 3"},
		{"ALTER TABLE t FORCE AUTO_INCREMENT = 3", true, "ALTER TABLE `t` FORCE AUTO_INCREMENT = 3"},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = DEFAULT;", true, "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = DEFAULT"},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = INPLACE;", true, "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = INPLACE"},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL , ALGORITHM = COPY;", true, "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = COPY"},
		// {"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE UTF8MB4_UNICODE_CI NOT NULL, ALGORITHM = INSTANT;", true, "ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL, ALGORITHM = INSTANT"},
		{"ALTER TABLE t CONVERT TO CHARACTER SET UTF8;", true, "ALTER TABLE `t` DEFAULT CHARACTER SET = UTF8"},
		{"ALTER TABLE t CONVERT TO CHARSET UTF8;", true, "ALTER TABLE `t` DEFAULT CHARACTER SET = UTF8"},
		{"ALTER TABLE t CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN;", true, "ALTER TABLE `t` CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN"},
		{"ALTER TABLE t CONVERT TO CHARSET UTF8 COLLATE UTF8_BIN;", true, "ALTER TABLE `t` CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN"},
		{"ALTER TABLE t FORCE", true, "ALTER TABLE `t` FORCE /* AlterTableForce is not supported */ "},
		{"ALTER TABLE t DROP INDEX;", false, "ALTER TABLE `t` DROP INDEX"},
		{"ALTER TABLE t DROP COLUMN a CASCADE", true, "ALTER TABLE `t` DROP COLUMN `a`"},
		{`ALTER TABLE testTableCompression COMPRESSION="LZ4";`, true, "ALTER TABLE `testTableCompression` COMPRESSION = 'LZ4'"},
		{`ALTER TABLE t1 COMPRESSION="zlib";`, true, "ALTER TABLE `t1` COMPRESSION = 'zlib'"},
		{"ALTER TABLE t1", true, "ALTER TABLE `t1`"},
		// {"ALTER TABLE t1 ,", false, ""},

		// For #6405
		{"ALTER TABLE t RENAME KEY a TO b;", true, "ALTER TABLE `t` RENAME INDEX `a` TO `b`"},
		{"ALTER TABLE t RENAME INDEX a TO b;", true, "ALTER TABLE `t` RENAME INDEX `a` TO `b`"},

		{"alter table t analyze partition a", true, "ANALYZE TABLE `t` PARTITION `a`"},
		{"alter table t analyze partition a with 4 buckets", true, "ANALYZE TABLE `t` PARTITION `a` WITH 4 BUCKETS"},
		{"alter table t analyze partition a index b", true, "ANALYZE TABLE `t` PARTITION `a` INDEX `b`"},
		{"alter table t analyze partition a index b with 4 buckets", true, "ANALYZE TABLE `t` PARTITION `a` INDEX `b` WITH 4 BUCKETS"},
		//
		{"alter table t force auto_random_base = 50", true, "ALTER TABLE `t` FORCE AUTO_RANDOM_BASE = 50"},
		{"alter table t auto_increment 30, force auto_random_base 40", true, "ALTER TABLE `t` AUTO_INCREMENT = 30, FORCE AUTO_RANDOM_BASE = 40"},
		// alter attributes
		{"ALTER TABLE t ATTRIBUTES='str'", true, "ALTER TABLE `t` ATTRIBUTES='str'"},
		{"ALTER TABLE t ATTRIBUTES='str1,str2'", true, "ALTER TABLE `t` ATTRIBUTES='str1,str2'"},
		{"ALTER TABLE t ATTRIBUTES=\"str1,str2\"", true, "ALTER TABLE `t` ATTRIBUTES='str1,str2'"},
		{"ALTER TABLE t ATTRIBUTES 'str1,str2'", true, "ALTER TABLE `t` ATTRIBUTES='str1,str2'"},
		{"ALTER TABLE t ATTRIBUTES \"str1,str2\"", true, "ALTER TABLE `t` ATTRIBUTES='str1,str2'"},
		{"ALTER TABLE t ATTRIBUTES=DEFAULT", true, "ALTER TABLE `t` ATTRIBUTES=DEFAULT"},
		{"ALTER TABLE t ATTRIBUTES=default", true, "ALTER TABLE `t` ATTRIBUTES=DEFAULT"},
		{"ALTER TABLE t ATTRIBUTES=DeFaUlT", true, "ALTER TABLE `t` ATTRIBUTES=DEFAULT"},
		{"ALTER TABLE t ATTRIBUTES", false, ""},
		{"ALTER TABLE t PARTITION p ATTRIBUTES='str'", true, "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str'"},
		{"ALTER TABLE t PARTITION p ATTRIBUTES='str1,str2'", true, "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str1,str2'"},
		{"ALTER TABLE t PARTITION p ATTRIBUTES=\"str1,str2\"", true, "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str1,str2'"},
		{"ALTER TABLE t PARTITION p ATTRIBUTES 'str1,str2'", true, "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str1,str2'"},
		{"ALTER TABLE t PARTITION p ATTRIBUTES \"str1,str2\"", true, "ALTER TABLE `t` PARTITION `p` ATTRIBUTES='str1,str2'"},
		{"ALTER TABLE t PARTITION p ATTRIBUTES=DEFAULT", true, "ALTER TABLE `t` PARTITION `p` ATTRIBUTES=DEFAULT"},
		{"ALTER TABLE t PARTITION p ATTRIBUTES=default", true, "ALTER TABLE `t` PARTITION `p` ATTRIBUTES=DEFAULT"},
		{"ALTER TABLE t PARTITION p ATTRIBUTES=DeFaUlT", true, "ALTER TABLE `t` PARTITION `p` ATTRIBUTES=DEFAULT"},
		{"ALTER TABLE t PARTITION p ATTRIBUTES", false, ""},
		// {"alter table t partition by hash(a)", true, "ALTER TABLE `t` PARTITION BY HASH (`a`) PARTITIONS 1"},
		// {"alter table t partition by range(a)", false, ""},
		// {"alter table t partition by range(a) (partition x values less than (75))", true, "ALTER TABLE `t` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))"},
		// {"alter table t comment 'cmt' partition by hash(a)", true, "ALTER TABLE `t` COMMENT = 'cmt' PARTITION BY HASH (`a`) PARTITIONS 1"},
		// {"alter table t enable keys, comment = 'cmt' partition by hash(a)", true, "ALTER TABLE `t` ENABLE KEYS, COMMENT = 'cmt' PARTITION BY HASH (`a`) PARTITIONS 1"},
		// {"alter table t enable keys, comment = 'cmt', partition by hash(a)", false, ""},
		// for TYPE/USING syntax
		{"create table t (a int, primary key type type btree (a));", true, "CREATE TABLE `t` (`a` INT,PRIMARY KEY `type`(`a`) USING BTREE)"},
		{"create table t (a int, primary key type btree (a));", false, ""},
		{"create table t (a int, primary key using btree (a));", true, "CREATE TABLE `t` (`a` INT,PRIMARY KEY(`a`) USING BTREE)"},
		{"create table t (a int, primary key (a) type btree);", true, "CREATE TABLE `t` (`a` INT,PRIMARY KEY(`a`) USING BTREE)"},
		// For create index statement
		{"CREATE INDEX idx ON t (a) STORING(a) WITH COLUMN GROUP(each column)", true, "CREATE INDEX `idx` ON `t` (`a`) STORING(`a`) WITH COLUMN GROUP(each column)"},
		{"CREATE INDEX idx ON t (a)", true, "CREATE INDEX `idx` ON `t` (`a`)"},
		{"CREATE UNIQUE INDEX idx ON t (a)", true, "CREATE UNIQUE INDEX `idx` ON `t` (`a`)"},
		{"CREATE SPATIAL INDEX idx ON t (a)", true, "CREATE SPATIAL INDEX `idx` ON `t` (`a`)"},
		{"CREATE FULLTEXT INDEX idx ON t (a)", true, "CREATE FULLTEXT INDEX `idx` ON `t` (`a`)"},
		{"CREATE GLOBAL INDEX idx ON t (a)", true, "CREATE GLOBAL INDEX `idx` ON `t` (`a`)"},
		{"CREATE UNIQUE GLOBAL INDEX idx ON t (a)", true, "CREATE UNIQUE GLOBAL INDEX `idx` ON `t` (`a`)"},
		{"CREATE INDEX idx ON t (a) GLOBAL", true, "CREATE INDEX `idx` ON `t` (`a`) GLOBAL"},
		{"CREATE INDEX idx ON t (a) USING HASH", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH"},
		{"CREATE INDEX idx ON t (a) COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) COMMENT 'foo'"},
		{"CREATE INDEX idx ON t (a) USING HASH COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'"},
		{"CREATE INDEX idx ON t (a) LOCK=NONE", true, "CREATE INDEX `idx` ON `t` (`a`)"},
		{"CREATE INDEX idx USING BTREE ON t (a) USING HASH COMMENT 'foo'", true, "CREATE INDEX `idx` ON `t` (`a`) USING HASH COMMENT 'foo'"},
		{"CREATE INDEX idx USING BTREE ON t (a)", true, "CREATE INDEX `idx` ON `t` (`a`) USING BTREE"},

		//For dorp index statement
		{"drop index a on t", true, "DROP INDEX `a` ON `t`"},
		{"drop index a on db.t", true, "DROP INDEX `a` ON `db`.`t`"},
		{"drop index a on db.`tb-ttb`", true, "DROP INDEX `a` ON `db`.`tb-ttb`"},
		{"drop index if exists a on t", true, "DROP INDEX IF EXISTS `a` ON `t`"},
		{"drop index if exists a on db.t", true, "DROP INDEX IF EXISTS `a` ON `db`.`t`"},
		{"drop index if exists a on db.`tb-ttb`", true, "DROP INDEX IF EXISTS `a` ON `db`.`tb-ttb`"},

		// for rename table statement
		{"RENAME TABLE t TO t1", true, "RENAME TABLE `t` TO `t1`"},
		{"RENAME TABLE t t1", false, "RENAME TABLE `t` TO `t1`"},
		{"RENAME TABLE d.t TO d1.t1", true, "RENAME TABLE `d`.`t` TO `d1`.`t1`"},
		{"RENAME TABLE t1 TO t2, t3 TO t4", true, "RENAME TABLE `t1` TO `t2`, `t3` TO `t4`"},

		// for truncate statement
		{"TRUNCATE TABLE t1", true, "TRUNCATE TABLE `t1`"},
		{"TRUNCATE t1", true, "TRUNCATE TABLE `t1`"},

		// for empty alert table index
		{"ALTER TABLE t ADD INDEX () ", false, ""},
		{"ALTER TABLE t ADD UNIQUE ()", false, ""},
		{"ALTER TABLE t ADD UNIQUE INDEX ()", false, ""},
		{"ALTER TABLE t ADD UNIQUE KEY ()", false, ""},

		// for issue 4538
		{"create table a (process double)", true, "CREATE TABLE `a` (`process` DOUBLE)"},

		// for issue 4740
		{"create table t (a int1, b int2, c int3, d int4, e int8)", true, "CREATE TABLE `t` (`a` TINYINT,`b` SMALLINT,`c` MEDIUMINT,`d` INT,`e` BIGINT)"},

		// for issue 5918
		{"create table t (lv long varchar null)", true, "CREATE TABLE `t` (`lv` MEDIUMTEXT NULL)"},

		// special table name
		{"CREATE TABLE cdp_test.`test2-1` (id int(11) DEFAULT NULL,key(id));", true, "CREATE TABLE `cdp_test`.`test2-1` (`id` INT(11) DEFAULT NULL,INDEX(`id`))"},
		{"CREATE TABLE miantiao (`扁豆焖面`       INT(11));", true, "CREATE TABLE `miantiao` (`扁豆焖面` INT(11))"},

		// for create table select
		{"CREATE TABLE bar (m INT)  SELECT n FROM foo;", true, "CREATE TABLE `bar` (`m` INT) AS SELECT `n` FROM `foo`"},
		{"CREATE TABLE bar (m INT) IGNORE SELECT n FROM foo;", true, "CREATE TABLE `bar` (`m` INT) IGNORE AS SELECT `n` FROM `foo`"},
		{"CREATE TABLE bar (m INT) REPLACE SELECT n FROM foo;", true, "CREATE TABLE `bar` (`m` INT) REPLACE AS SELECT `n` FROM `foo`"},

		// for generated column definition
		// {"create table t (a timestamp, b timestamp as (a) not null on update current_timestamp);", false, ""},
		// {"create table t (a bigint, b bigint as (a) primary key auto_increment);", false, ""},
		// {"create table t (a bigint, b bigint as (a) not null default 10);", false, ""},
		{"create table t (a bigint, b bigint as (a+1) not null);", true, "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL)"},
		{"create table t (a bigint, b bigint as (a+1) not null);", true, "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL)"},
		{"create table t (a bigint, b bigint as (a+1) not null comment 'ttt');", true, "CREATE TABLE `t` (`a` BIGINT,`b` BIGINT GENERATED ALWAYS AS(`a`+1) VIRTUAL NOT NULL COMMENT 'ttt')"},
		// {"alter table t add column (f timestamp as (a+1) default '2019-01-01 11:11:11');", false, ""},
		// {"alter table t modify column f int as (a+1) default 55;", false, ""},

		// for recover table
		// {"recover table by job 11", true, "RECOVER TABLE BY JOB 11"},
		// {"recover table by job 11,12,13", false, ""},
		// {"recover table by job", false, ""},
		// {"recover table t1", true, "RECOVER TABLE `t1`"},
		// {"recover table t1,t2", false, ""},
		// {"recover table ", false, ""},
		// {"recover table t1 100", true, "RECOVER TABLE `t1` 100"},
		// {"recover table t1 abc", false, ""},
		// for clustered index
		{"create table t (a int, b varchar(255), primary key(b, a) clustered)", true, "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255),PRIMARY KEY(`b`, `a`) CLUSTERED)"},
		{"create table t (a int, b varchar(255), primary key(b, a) nonclustered)", true, "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255),PRIMARY KEY(`b`, `a`) NONCLUSTERED)"},
		{"create table t (a int primary key nonclustered, b varchar(255))", true, "CREATE TABLE `t` (`a` INT PRIMARY KEY NONCLUSTERED,`b` VARCHAR(255))"},
		{"create table t (a int, b varchar(255) primary key clustered)", true, "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255) PRIMARY KEY CLUSTERED)"},
		{"create table t (a int, b varchar(255) default 'a' primary key clustered)", true, "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255) DEFAULT _UTF8MB4'a' PRIMARY KEY CLUSTERED)"},
		{"create table t (a int, b varchar(255) primary key nonclustered, primary key(b, a) nonclustered)", true, "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255) PRIMARY KEY NONCLUSTERED,PRIMARY KEY(`b`, `a`) NONCLUSTERED)"},
		{"create table t (a int, b varchar(255), primary key(b, a) using RTREE nonclustered)", true, "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255),PRIMARY KEY(`b`, `a`) NONCLUSTERED USING RTREE)"},
		{"create table t (a int, b varchar(255), primary key(b, a) using RTREE clustered nonclustered)", true, "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255),PRIMARY KEY(`b`, `a`) NONCLUSTERED USING RTREE)"},
		{"create table t (a int, b varchar(255), primary key(b, a) using RTREE nonclustered clustered)", true, "CREATE TABLE `t` (`a` INT,`b` VARCHAR(255),PRIMARY KEY(`b`, `a`) CLUSTERED USING RTREE)"},
		{"create table t (a int, b varchar(255) clustered primary key)", false, ""},
		{"create table t (a int, b varchar(255) primary key nonclustered clustered)", false, ""},
		{"alter table t add primary key (`a`, `b`) clustered", true, "ALTER TABLE `t` ADD PRIMARY KEY(`a`, `b`) CLUSTERED"},
		{"alter table t add primary key (`a`, `b`) nonclustered", true, "ALTER TABLE `t` ADD PRIMARY KEY(`a`, `b`) NONCLUSTERED"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestOptimizerHints(c *C) {
	parser := New()
	stmt, _, err := parser.Parse("select /*+ tidb_SMJ(T1,t2) tidb_smj(T3,t4) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	c.Assert(err, IsNil)
	selectStmt := stmt[0].(*ast.SelectStmt)

	hints := selectStmt.TableHints
	c.Assert(len(hints), Equals, 2)
	c.Assert(hints[0].HintName.L, Equals, "tidb_smj")
	c.Assert(len(hints[0].Tables), Equals, 2)
	c.Assert(hints[0].Tables[0].TableName.L, Equals, "t1")
	c.Assert(hints[0].Tables[1].TableName.L, Equals, "t2")

	c.Assert(hints[1].HintName.L, Equals, "tidb_smj")
	c.Assert(hints[1].Tables[0].TableName.L, Equals, "t3")
	c.Assert(hints[1].Tables[1].TableName.L, Equals, "t4")

	c.Assert(len(selectStmt.TableHints), Equals, 2)

	stmt, _, err = parser.Parse("select /*+ TIDB_INLJ(t1, T2) tidb_inlj(t3, t4) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	c.Assert(err, IsNil)
	selectStmt = stmt[0].(*ast.SelectStmt)

	hints = selectStmt.TableHints
	c.Assert(len(hints), Equals, 2)
	c.Assert(hints[0].HintName.L, Equals, "tidb_inlj")
	c.Assert(len(hints[0].Tables), Equals, 2)
	c.Assert(hints[0].Tables[0].TableName.L, Equals, "t1")
	c.Assert(hints[0].Tables[1].TableName.L, Equals, "t2")

	c.Assert(hints[1].HintName.L, Equals, "tidb_inlj")
	c.Assert(hints[1].Tables[0].TableName.L, Equals, "t3")
	c.Assert(hints[1].Tables[1].TableName.L, Equals, "t4")

	stmt, _, err = parser.Parse("select /*+ TIDB_HJ(t1, T2) tidb_hj(t3, t4) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	c.Assert(err, IsNil)
	selectStmt = stmt[0].(*ast.SelectStmt)

	hints = selectStmt.TableHints
	c.Assert(len(hints), Equals, 2)
	c.Assert(hints[0].HintName.L, Equals, "tidb_hj")
	c.Assert(len(hints[0].Tables), Equals, 2)
	c.Assert(hints[0].Tables[0].TableName.L, Equals, "t1")
	c.Assert(hints[0].Tables[1].TableName.L, Equals, "t2")

	c.Assert(hints[1].HintName.L, Equals, "tidb_hj")
	c.Assert(hints[1].Tables[0].TableName.L, Equals, "t3")
	c.Assert(hints[1].Tables[1].TableName.L, Equals, "t4")

	// Test MAX_EXECUTION_TIME
	queries := []string{
		"SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t1 INNER JOIN t2 where t1.c1 = t2.c1",
		// "SELECT /*+ MAX_EXECUTION_TIME(1000) */ 1",
		// "SELECT /*+ MAX_EXECUTION_TIME(1000) */ SLEEP(20)",
		// "SELECT /*+ MAX_EXECUTION_TIME(1000) */ 1 FROM DUAL",
	}
	for i, query := range queries {
		stmt, _, err = parser.Parse(query, "", "")
		c.Assert(err, IsNil)
		selectStmt = stmt[0].(*ast.SelectStmt)
		hints = selectStmt.TableHints
		c.Assert(len(hints), Equals, 1)
		c.Assert(hints[0].HintName.L, Equals, "max_execution_time", Commentf("case", i))
		c.Assert(hints[0].HintData.(uint64), Equals, uint64(1000))
	}
}

func (s *testParserSuite) TestType(c *C) {
	table := []testCase{
		// for time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true, "CREATE TABLE `t` (`c1` TIME(2),`c2` DATETIME(2),`c3` TIMESTAMP(2))"},

		// for hexadecimal
		{"select x'0a', X'11', 0x11", true, "SELECT x'0a',x'11',x'11'"},
		{"select x'13181C76734725455A'", true, "SELECT x'13181c76734725455a'"},
		{"select x'0xaa'", false, ""},
		{"select 0X11", false, ""},
		{"select 0x4920616D2061206C6F6E672068657820737472696E67", true, "SELECT x'4920616d2061206c6f6e672068657820737472696e67'"},

		// for bit
		{"select 0b01, 0b0, b'11', B'11'", true, "SELECT b'1',b'0',b'11',b'11'"},
		// 0B01 and 0b21 are identifiers, the following two statement could parse.
		// {"select 0B01", false, ""},
		// {"select 0b21", false, ""},

		// for enum and set type
		{"create table t (c1 enum('a', 'b'), c2 set('a', 'b'))", true, "CREATE TABLE `t` (`c1` ENUM('a','b'),`c2` SET('a','b'))"},
		{"create table t (c1 enum)", false, ""},
		{"create table t (c1 set)", false, ""},

		// for blob and text field length
		{"create table t (c1 blob(1024), c2 text(1024))", true, "CREATE TABLE `t` (`c1` BLOB(1024),`c2` TEXT(1024))"},

		// for year
		{"create table t (y year(4), y1 year)", true, "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)"},
		{"create table t (y year(4) unsigned zerofill zerofill, y1 year signed unsigned zerofill)", true, "CREATE TABLE `t` (`y` YEAR(4),`y1` YEAR)"},

		// for national
		{"create table t (c1 national char(2), c2 national varchar(2))", true, "CREATE TABLE `t` (`c1` CHAR(2),`c2` VARCHAR(2))"},

		// for json type
		{`create table t (a JSON);`, true, "CREATE TABLE `t` (`a` JSON)"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestPrivilege(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// for create user
		// {`CREATE USER 'ttt' REQUIRE X509;`, true, "CREATE USER `ttt`@`%` REQUIRE X509"},
		// {`CREATE USER 'ttt' REQUIRE SSL;`, true, "CREATE USER `ttt`@`%` REQUIRE SSL"},
		// {`CREATE USER 'ttt' REQUIRE NONE;`, true, "CREATE USER `ttt`@`%` REQUIRE NONE"},
		// {`CREATE USER 'ttt' REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA';`, true, "CREATE USER `ttt`@`%` REQUIRE ISSUER '/C=SE/ST=Stockholm/L=Stockholm/O=MySQL/CN=CA/emailAddress=ca@example.com' AND CIPHER 'EDH-RSA-DES-CBC3-SHA'"},
		// {`CREATE USER 'ttt' WITH MAX_QUERIES_PER_HOUR 2;`, true, "CREATE USER `ttt`@`%` WITH MAX_QUERIES_PER_HOUR 2"},
		// {`CREATE USER 'ttt'@'localhost' REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 MAX_UPDATES_PER_HOUR 10 PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK;`, true, "CREATE USER `ttt`@`localhost` REQUIRE NONE WITH MAX_QUERIES_PER_HOUR 1 MAX_UPDATES_PER_HOUR 10 PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"},
		// {`CREATE USER 'u1'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK ;`, true, "CREATE USER `u1`@`%` IDENTIFIED BY PASSWORD '' REQUIRE NONE PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"},
		{`CREATE USER 'test'`, true, "CREATE USER `test`@`%`"},
		{`CREATE USER test`, true, "CREATE USER `test`@`%`"},
		{"CREATE USER `test`", true, "CREATE USER `test`@`%`"},
		{"CREATE USER test-user", false, ""},
		{"CREATE USER test.user", false, ""},
		{"CREATE USER 'test-user'", true, "CREATE USER `test-user`@`%`"},
		{"CREATE USER `test-user`", true, "CREATE USER `test-user`@`%`"},
		{"CREATE USER test.user", false, ""},
		{"CREATE USER 'test.user'", true, "CREATE USER `test.user`@`%`"},
		{"CREATE USER `test.user`", true, "CREATE USER `test.user`@`%`"},
		{"CREATE USER uesr1@localhost", true, "CREATE USER `uesr1`@`localhost`"},
		{"CREATE USER `uesr1`@localhost", true, "CREATE USER `uesr1`@`localhost`"},
		{"CREATE USER uesr1@`localhost`", true, "CREATE USER `uesr1`@`localhost`"},
		{"CREATE USER `uesr1`@`localhost`", true, "CREATE USER `uesr1`@`localhost`"},
		{"CREATE USER 'uesr1'@localhost", true, "CREATE USER `uesr1`@`localhost`"},
		{"CREATE USER uesr1@'localhost'", true, "CREATE USER `uesr1`@`localhost`"},
		{"CREATE USER 'uesr1'@'localhost'", true, "CREATE USER `uesr1`@`localhost`"},
		{"CREATE USER 'uesr1'@`localhost`", true, "CREATE USER `uesr1`@`localhost`"},
		{"CREATE USER `uesr1`@'localhost'", true, "CREATE USER `uesr1`@`localhost`"},
		// {"CREATE ROLE `test-role`, `role1`@'localhost'", true, "CREATE ROLE `test-role`@`%`, `role1`@`localhost`"},
		// {"CREATE ROLE `test-role`", true, "CREATE ROLE `test-role`@`%`"},
		// {"CREATE ROLE role1", true, "CREATE ROLE `role1`@`%`"},
		// {"CREATE ROLE `role1`@'localhost'", true, "CREATE ROLE `role1`@`localhost`"},
		{"create user 'bug19354014user'@'%' identified WITH mysql_native_password", true, "CREATE USER `bug19354014user`@`%`"},
		{"create user 'bug19354014user'@'%' identified WITH mysql_native_password by 'new-password'", true, "CREATE USER `bug19354014user`@`%` IDENTIFIED BY 'new-password'"},
		{"create user 'bug19354014user'@'%' identified WITH mysql_native_password as 'hashstring'", true, "CREATE USER `bug19354014user`@`%` IDENTIFIED BY PASSWORD 'hashstring'"},
		{`CREATE USER IF NOT EXISTS 'root'@'localhost' IDENTIFIED BY 'new-password'`, true, "CREATE USER IF NOT EXISTS `root`@`localhost` IDENTIFIED BY 'new-password'"},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY 'new-password'`, true, "CREATE USER `root`@`localhost` IDENTIFIED BY 'new-password'"},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY PASSWORD 'hashstring'`, true, "CREATE USER `root`@`localhost` IDENTIFIED BY PASSWORD 'hashstring'"},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY 'new-password', 'root'@'127.0.0.1' IDENTIFIED BY PASSWORD 'hashstring'`, true, "CREATE USER `root`@`localhost` IDENTIFIED BY 'new-password', `root`@`127.0.0.1` IDENTIFIED BY PASSWORD 'hashstring'"},
		{`ALTER USER IF EXISTS 'root'@'localhost' IDENTIFIED BY 'new-password'`, true, "ALTER USER IF EXISTS `root`@`localhost` IDENTIFIED BY 'new-password'"},
		{`ALTER USER 'root'@'localhost' IDENTIFIED BY 'new-password'`, true, "ALTER USER `root`@`localhost` IDENTIFIED BY 'new-password'"},
		{`ALTER USER 'root'@'localhost' IDENTIFIED BY PASSWORD 'hashstring'`, true, "ALTER USER `root`@`localhost` IDENTIFIED BY PASSWORD 'hashstring'"},
		{`ALTER USER 'root'@'localhost' IDENTIFIED BY 'new-password', 'root'@'127.0.0.1' IDENTIFIED BY PASSWORD 'hashstring'`, true, "ALTER USER `root`@`localhost` IDENTIFIED BY 'new-password', `root`@`127.0.0.1` IDENTIFIED BY PASSWORD 'hashstring'"},
		{`ALTER USER USER() IDENTIFIED BY 'new-password'`, true, "ALTER USER USER() IDENTIFIED BY 'new-password'"},
		{`ALTER USER IF EXISTS USER() IDENTIFIED BY 'new-password'`, true, "ALTER USER IF EXISTS USER() IDENTIFIED BY 'new-password'"},
		{`DROP USER 'root'@'localhost', 'root1'@'localhost'`, true, "DROP USER `root`@`localhost`, `root1`@`localhost`"},
		{`DROP USER IF EXISTS 'root'@'localhost'`, true, "DROP USER IF EXISTS `root`@`localhost`"},
		// {`DROP ROLE 'role'@'localhost', 'role1'@'localhost'`, true, "DROP ROLE `role`@`localhost`, `role1`@`localhost`"},
		// {`DROP ROLE 'administrator', 'developer';`, true, "DROP ROLE `administrator`@`%`, `developer`@`%`"},
		// {`DROP ROLE IF EXISTS 'role'@'localhost'`, true, "DROP ROLE IF EXISTS `role`@`localhost`"},

		// for grant statement
		{"GRANT ALL ON db1.* TO 'jeffrey'@'localhost';", true, "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost`"},
		{"GRANT ALL ON TABLE db1.* TO 'jeffrey'@'localhost';", true, "GRANT ALL ON TABLE `db1`.* TO `jeffrey`@`localhost`"},
		{"GRANT ALL ON db1.* TO 'jeffrey'@'localhost' WITH GRANT OPTION;", true, "GRANT ALL ON `db1`.* TO `jeffrey`@`localhost` WITH GRANT OPTION"},
		{"GRANT SELECT ON db2.invoice TO 'jeffrey'@'localhost';", true, "GRANT SELECT ON `db2`.`invoice` TO `jeffrey`@`localhost`"},
		{"GRANT ALL ON *.* TO 'someuser'@'somehost';", true, "GRANT ALL ON *.* TO `someuser`@`somehost`"},
		{"GRANT SELECT, INSERT ON *.* TO 'someuser'@'somehost';", true, "GRANT SELECT, INSERT ON *.* TO `someuser`@`somehost`"},
		{"GRANT ALL ON mydb.* TO 'someuser'@'somehost';", true, "GRANT ALL ON `mydb`.* TO `someuser`@`somehost`"},
		{"GRANT SELECT, INSERT ON mydb.* TO 'someuser'@'somehost';", true, "GRANT SELECT, INSERT ON `mydb`.* TO `someuser`@`somehost`"},
		{"GRANT ALL ON mydb.mytbl TO 'someuser'@'somehost';", true, "GRANT ALL ON `mydb`.`mytbl` TO `someuser`@`somehost`"},
		{"GRANT SELECT, INSERT ON mydb.mytbl TO 'someuser'@'somehost';", true, "GRANT SELECT, INSERT ON `mydb`.`mytbl` TO `someuser`@`somehost`"},
		{"GRANT SELECT (col1), INSERT (col1,col2) ON mydb.mytbl TO 'someuser'@'somehost';", true, "GRANT SELECT (`col1`), INSERT (`col1`,`col2`) ON `mydb`.`mytbl` TO `someuser`@`somehost`"},
		{"grant all privileges on zabbix.* to 'zabbix'@'localhost' identified by 'password';", true, "GRANT ALL ON `zabbix`.* TO `zabbix`@`localhost` IDENTIFIED BY 'password'"},
		{"GRANT SELECT ON test.* to 'test'", true, "GRANT SELECT ON `test`.* TO `test`@`%`"}, // For issue 2654.
		{"grant PROCESS,usage, REPLICATION SLAVE, REPLICATION CLIENT on *.* to 'xxxxxxxxxx'@'%' identified by password 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'", true, "GRANT PROCESS /* UNSUPPORTED TYPE */ /* UNSUPPORTED TYPE */ /* UNSUPPORTED TYPE */ ON *.* TO `xxxxxxxxxx`@`%` IDENTIFIED BY PASSWORD 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'"}, // For issue 4865
		{"/* rds internal mark */ GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, RELOAD, PROCESS, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES,      EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT,      TRIGGER on *.* to 'root2'@'%' identified by password '*sdsadsdsadssadsadsadsadsada' with grant option", true, "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES /* UNSUPPORTED TYPE */, PROCESS, INDEX, ALTER /* UNSUPPORTED TYPE */ /* UNSUPPORTED TYPE */, EXECUTE /* UNSUPPORTED TYPE */ /* UNSUPPORTED TYPE */, CREATE VIEW, SHOW VIEW /* UNSUPPORTED TYPE */ /* UNSUPPORTED TYPE */, CREATE USER /* UNSUPPORTED TYPE */, TRIGGER ON *.* TO `root2`@`%` IDENTIFIED BY PASSWORD '*sdsadsdsadssadsadsadsadsada' WITH GRANT OPTION"},
		// {"GRANT 'role1', 'role2' TO 'user1'@'localhost', 'user2'@'localhost';", true, "GRANT `role1`@`%`, `role2`@`%` TO `user1`@`localhost`, `user2`@`localhost`"},
		// {"GRANT 'u1' TO 'u1';", true, "GRANT `u1`@`%` TO `u1`@`%`"},
		// {"GRANT 'app_developer' TO 'dev1'@'localhost';", true, "GRANT `app_developer`@`%` TO `dev1`@`localhost`"},

		// for revoke statement
		{"REVOKE ALL ON db1.* FROM 'jeffrey'@'localhost';", true, "REVOKE ALL ON `db1`.* FROM `jeffrey`@`localhost`"},
		{"REVOKE SELECT ON db2.invoice FROM 'jeffrey'@'localhost';", true, "REVOKE SELECT ON `db2`.`invoice` FROM `jeffrey`@`localhost`"},
		{"REVOKE ALL ON *.* FROM 'someuser'@'somehost';", true, "REVOKE ALL ON *.* FROM `someuser`@`somehost`"},
		{"REVOKE SELECT, INSERT ON *.* FROM 'someuser'@'somehost';", true, "REVOKE SELECT, INSERT ON *.* FROM `someuser`@`somehost`"},
		{"REVOKE ALL ON mydb.* FROM 'someuser'@'somehost';", true, "REVOKE ALL ON `mydb`.* FROM `someuser`@`somehost`"},
		{"REVOKE SELECT, INSERT ON mydb.* FROM 'someuser'@'somehost';", true, "REVOKE SELECT, INSERT ON `mydb`.* FROM `someuser`@`somehost`"},
		{"REVOKE ALL ON mydb.mytbl FROM 'someuser'@'somehost';", true, "REVOKE ALL ON `mydb`.`mytbl` FROM `someuser`@`somehost`"},
		{"REVOKE SELECT, INSERT ON mydb.mytbl FROM 'someuser'@'somehost';", true, "REVOKE SELECT, INSERT ON `mydb`.`mytbl` FROM `someuser`@`somehost`"},
		{"REVOKE SELECT (col1), INSERT (col1,col2) ON mydb.mytbl FROM 'someuser'@'somehost';", true, "REVOKE SELECT (`col1`), INSERT (`col1`,`col2`) ON `mydb`.`mytbl` FROM `someuser`@`somehost`"},
		{"REVOKE all privileges on zabbix.* FROM 'zabbix'@'localhost' identified by 'password';", true, "REVOKE ALL ON `zabbix`.* FROM `zabbix`@`localhost` IDENTIFIED BY 'password'"},
		// {"REVOKE 'role1', 'role2' FROM 'user1'@'localhost', 'user2'@'localhost';", true, "REVOKE `role1`@`%`, `role2`@`%` FROM `user1`@`localhost`, `user2`@`localhost`"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestComment(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{"create table t (c int comment 'comment')", true, "CREATE TABLE `t` (`c` INT COMMENT 'comment')"},
		{"create table t (c int) comment = 'comment'", true, "CREATE TABLE `t` (`c` INT) COMMENT = 'comment'"},
		{"create table t (c int) comment 'comment'", true, "CREATE TABLE `t` (`c` INT) COMMENT = 'comment'"},
		{"create table t (c int) comment comment", false, ""},
		{"create table t (comment text)", true, "CREATE TABLE `t` (`comment` TEXT)"},
		{"START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */", true, "START TRANSACTION"},
		// for comment in query
		{"/*comment*/ /*comment*/ select c /* this is a comment */ from t;", true, "SELECT `c` FROM `t`"},
		// for unclosed comment
		{"delete from t where a = 7 or 1=1/*' and b = 'p'", false, ""},

		// {"create table t (ssl int)", false, ""},
		// {"create table t (require int)", false, ""},
		{"create table t (account int)", true, "CREATE TABLE `t` (`account` INT)"},
		{"create table t (expire int)", true, "CREATE TABLE `t` (`expire` INT)"},
		{"create table t (cipher int)", true, "CREATE TABLE `t` (`cipher` INT)"},
		{"create table t (issuer int)", true, "CREATE TABLE `t` (`issuer` INT)"},
		{"create table t (never int)", true, "CREATE TABLE `t` (`never` INT)"},
		{"create table t (subject int)", true, "CREATE TABLE `t` (`subject` INT)"},
		{"create table t (x509 int)", true, "CREATE TABLE `t` (`x509` INT)"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestParserErrMsg(c *C) {
	commentMsgCases := []testErrMsgCase{
		{"delete from t where a = 7 or 1=1/*' and b = 'p'", errors.New("near '/*' and b = 'p'' at line 1")},
		{"delete from t where a = 7 or\n 1=1/*' and b = 'p'", errors.New("near '/*' and b = 'p'' at line 2")},
		{"select 1/*", errors.New("near '/*' at line 1")},
		{"select 1/* comment */", nil},
	}
	funcCallMsgCases := []testErrMsgCase{
		{"select a.b()", nil},
		{"SELECT foo.bar('baz');", nil},
	}
	s.RunErrMsgTest(c, commentMsgCases)
	s.RunErrMsgTest(c, funcCallMsgCases)
}

type subqueryChecker struct {
	text string
	c    *C
}

// Enter implements ast.Visitor interface.
func (sc *subqueryChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	if expr, ok := inNode.(*ast.SubqueryExpr); ok {
		sc.c.Assert(expr.Query.Text(), Equals, sc.text)
		return inNode, true
	}
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (sc *subqueryChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	return inNode, true
}

func (s *testParserSuite) TestSubquery(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// for compare subquery
		{"SELECT 1 > (select 1)", true, "SELECT 1>(SELECT 1)"},
		{"SELECT 1 > ANY (select 1)", true, "SELECT 1>ANY (SELECT 1)"},
		{"SELECT 1 > ALL (select 1)", true, "SELECT 1>ALL (SELECT 1)"},
		{"SELECT 1 > SOME (select 1)", true, "SELECT 1>ANY (SELECT 1)"},

		// for exists subquery
		{"SELECT EXISTS select 1", false, ""},
		{"SELECT EXISTS (select 1)", true, "SELECT EXISTS (SELECT 1)"},
		{"SELECT + EXISTS (select 1)", true, "SELECT +EXISTS (SELECT 1)"},
		{"SELECT - EXISTS (select 1)", true, "SELECT -EXISTS (SELECT 1)"},
		{"SELECT NOT EXISTS (select 1)", true, "SELECT NOT EXISTS (SELECT 1)"},
		{"SELECT + NOT EXISTS (select 1)", false, ""},
		{"SELECT - NOT EXISTS (select 1)", false, ""},
		{"SELECT * FROM t where t.a in (select a from t limit 1, 10)", true, "SELECT * FROM `t` WHERE `t`.`a` IN (SELECT `a` FROM `t` LIMIT 1,10)"},
		{"SELECT * FROM t where t.a in ((select a from t limit 1, 10))", true, "SELECT * FROM `t` WHERE `t`.`a` IN ((SELECT `a` FROM `t` LIMIT 1,10))"},
		{"SELECT * FROM t where t.a in ((select a from t limit 1, 10), 1)", true, "SELECT * FROM `t` WHERE `t`.`a` IN ((SELECT `a` FROM `t` LIMIT 1,10),1)"},
		{"select * from ((select a from t) t1 join t t2) join t3", true, "SELECT * FROM ((SELECT `a` FROM `t`) AS `t1` JOIN `t` AS `t2`) JOIN `t3`"},
		{"SELECT t1.a AS a FROM ((SELECT a FROM t) AS t1)", true, "SELECT `t1`.`a` AS `a` FROM (SELECT `a` FROM `t`) AS `t1`"},
		{"select count(*) from (select a, b from x1 union all select a, b from x3 union all (select x1.a, x3.b from (select * from x3 union all select * from x2) x3 left join x1 on x3.a = x1.b))", true, "SELECT COUNT(1) FROM (SELECT `a`,`b` FROM `x1` UNION ALL SELECT `a`,`b` FROM `x3` UNION ALL (SELECT `x1`.`a`,`x3`.`b` FROM (SELECT * FROM `x3` UNION ALL SELECT * FROM `x2`) AS `x3` LEFT JOIN `x1` ON `x3`.`a`=`x1`.`b`))"},
		{"(SELECT 1 a,3 b) UNION (SELECT 2,1) ORDER BY (SELECT 2)", true, "(SELECT 1 AS `a`,3 AS `b`) UNION (SELECT 2,1) ORDER BY (SELECT 2)"},
		{"((select * from t1)) union (select * from t1)", true, "(SELECT * FROM `t1`) UNION (SELECT * FROM `t1`)"},
		{"(((select * from t1))) union (select * from t1)", true, "(SELECT * FROM `t1`) UNION (SELECT * FROM `t1`)"},
		{"select * from (((select * from t1)) union (select * from t1) union (select * from t1)) a", true, "SELECT * FROM ((SELECT * FROM `t1`) UNION (SELECT * FROM `t1`) UNION (SELECT * FROM `t1`)) AS `a`"},
		{"SELECT COUNT(*) FROM plan_executions WHERE (EXISTS((SELECT * FROM triggers WHERE plan_executions.trigger_id=triggers.id AND triggers.type='CRON')))", true, "SELECT COUNT(1) FROM `plan_executions` WHERE (EXISTS (SELECT * FROM `triggers` WHERE `plan_executions`.`trigger_id`=`triggers`.`id` AND `triggers`.`type`=_UTF8MB4'CRON'))"},
		{"select exists((select 1));", true, "SELECT EXISTS (SELECT 1)"},
		{"select * from ((SELECT 1 a,3 b) UNION (SELECT 2,1) ORDER BY (SELECT 2)) t order by a,b", true, "SELECT * FROM ((SELECT 1 AS `a`,3 AS `b`) UNION (SELECT 2,1) ORDER BY (SELECT 2)) AS `t` ORDER BY `a`,`b`"},
		{"select (select * from t1 where a != t.a union all (select * from t2 where a != t.a) order by a limit 1) from t1 t", true, "SELECT (SELECT * FROM `t1` WHERE `a`!=`t`.`a` UNION ALL (SELECT * FROM `t2` WHERE `a`!=`t`.`a`) ORDER BY `a` LIMIT 1) FROM `t1` AS `t`"},
	}
	s.RunTest(c, table, false)

	tests := []struct {
		input string
		text  string
	}{
		{"SELECT 1 > (select 1)", "select 1"},
		{"SELECT 1 > (select 1 union select 2)", "select 1 union select 2"},
	}
	parser := New()
	for _, t := range tests {
		stmt, err := parser.ParseOneStmt(t.input, "", "")
		c.Assert(err, IsNil)
		stmt.Accept(&subqueryChecker{
			text: t.text,
			c:    c,
		})
	}
}
func (s *testParserSuite) TestSetOperator(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{"select c1 from t1 union select c2 from t2", true, "SELECT `c1` FROM `t1` UNION SELECT `c2` FROM `t2`"},
		{"select c1 from t1 union (select c2 from t2)", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`)"},
		{"select c1 from t1 union (select c2 from t2) order by c1", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) ORDER BY `c1`"},
		{"select c1 from t1 union select c2 from t2 order by c2", true, "SELECT `c1` FROM `t1` UNION SELECT `c2` FROM `t2` ORDER BY `c2`"},
		{"select c1 from t1 union (select c2 from t2) limit 1", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) LIMIT 1"},
		{"select c1 from t1 union (select c2 from t2) limit 1, 1", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) LIMIT 1,1"},
		{"select c1 from t1 union (select c2 from t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) union distinct select c2 from t2", true, "(SELECT `c1` FROM `t1`) UNION SELECT `c2` FROM `t2`"},
		{"(select c1 from t1) union distinctrow select c2 from t2", true, "(SELECT `c1` FROM `t1`) UNION SELECT `c2` FROM `t2`"},
		{"(select c1 from t1) union all select c2 from t2", true, "(SELECT `c1` FROM `t1`) UNION ALL SELECT `c2` FROM `t2`"},
		{"(select c1 from t1) union distinct all select c2 from t2", false, ""},
		{"(select c1 from t1) union distinctrow all select c2 from t2", false, ""},
		{"(select c1 from t1) union (select c2 from t2) order by c1 union select c3 from t3", false, ""},
		{"(select c1 from t1) union (select c2 from t2) limit 1 union select c3 from t3", false, ""},
		{"(select c1 from t1) union select c2 from t2 union (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) UNION SELECT `c2` FROM `t2` UNION (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"select (select 1 union select 1) as a", true, "SELECT (SELECT 1 UNION SELECT 1) AS `a`"},
		{"select * from (select 1 union select 2) as a", true, "SELECT * FROM (SELECT 1 UNION SELECT 2) AS `a`"},
		{"insert into t select c1 from t1 union select c2 from t2", true, "INSERT INTO `t` SELECT `c1` FROM `t1` UNION SELECT `c2` FROM `t2`"},
		{"insert into t (c) select c1 from t1 union select c2 from t2", true, "INSERT INTO `t` (`c`) SELECT `c1` FROM `t1` UNION SELECT `c2` FROM `t2`"},
		{"select 2 as a from dual union select 1 as b from dual order by a", true, "SELECT 2 AS `a` UNION SELECT 1 AS `b` ORDER BY `a`"},
		{"table t1 union table t2", true, "TABLE `t1` UNION TABLE `t2`"},
		{"table t1 union (table t2)", true, "TABLE `t1` UNION (TABLE `t2`)"},
		{"table t1 union select * from t2", true, "TABLE `t1` UNION SELECT * FROM `t2`"},
		{"select * from t1 union table t2", true, "SELECT * FROM `t1` UNION TABLE `t2`"},
		{"table t1 union (select c2 from t2) order by c1 limit 1", true, "TABLE `t1` UNION (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"},
		{"select c1 from t1 union (table t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` UNION (TABLE `t2`) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) union table t2 union (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) UNION TABLE `t2` UNION (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"(table t1) union select c2 from t2 union (table t3) order by c1 limit 1", true, "(TABLE `t1`) UNION SELECT `c2` FROM `t2` UNION (TABLE `t3`) ORDER BY `c1` LIMIT 1"},
		{"values row(1,-2,3), row(5,7,9) union values row(1,-2,3), row(5,7,9)", true, "VALUES ROW(1,-2,3), ROW(5,7,9) UNION VALUES ROW(1,-2,3), ROW(5,7,9)"},
		{"values row(1,-2,3), row(5,7,9) union (values row(1,-2,3), row(5,7,9))", true, "VALUES ROW(1,-2,3), ROW(5,7,9) UNION (VALUES ROW(1,-2,3), ROW(5,7,9))"},
		{"values row(1,-2,3), row(5,7,9) union select * from t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) UNION SELECT * FROM `t`"},
		{"values row(1,-2,3), row(5,7,9) union table t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) UNION TABLE `t`"},
		{"select * from t union values row(1,-2,3), row(5,7,9)", true, "SELECT * FROM `t` UNION VALUES ROW(1,-2,3), ROW(5,7,9)"},
		{"table t union values row(1,-2,3), row(5,7,9)", true, "TABLE `t` UNION VALUES ROW(1,-2,3), ROW(5,7,9)"},
		// except
		{"select c1 from t1 except select c2 from t2", true, "SELECT `c1` FROM `t1` EXCEPT SELECT `c2` FROM `t2`"},
		{"select c1 from t1 except (select c2 from t2)", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`)"},
		{"select c1 from t1 except (select c2 from t2) order by c1", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`) ORDER BY `c1`"},
		{"select c1 from t1 except select c2 from t2 order by c2", true, "SELECT `c1` FROM `t1` EXCEPT SELECT `c2` FROM `t2` ORDER BY `c2`"},
		{"select c1 from t1 except (select c2 from t2) limit 1", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`) LIMIT 1"},
		{"select c1 from t1 except (select c2 from t2) limit 1, 1", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`) LIMIT 1,1"},
		{"select c1 from t1 except (select c2 from t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` EXCEPT (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) except (select c2 from t2) order by c1 except select c3 from t3", false, ""},
		{"(select c1 from t1) except (select c2 from t2) limit 1 except select c3 from t3", false, ""},
		{"(select c1 from t1) except select c2 from t2 except (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) EXCEPT SELECT `c2` FROM `t2` EXCEPT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"select (select 1 except select 1) as a", true, "SELECT (SELECT 1 EXCEPT SELECT 1) AS `a`"},
		{"select * from (select 1 except select 2) as a", true, "SELECT * FROM (SELECT 1 EXCEPT SELECT 2) AS `a`"},
		{"insert into t select c1 from t1 except select c2 from t2", true, "INSERT INTO `t` SELECT `c1` FROM `t1` EXCEPT SELECT `c2` FROM `t2`"},
		{"insert into t (c) select c1 from t1 except select c2 from t2", true, "INSERT INTO `t` (`c`) SELECT `c1` FROM `t1` EXCEPT SELECT `c2` FROM `t2`"},
		{"select 2 as a from dual except select 1 as b from dual order by a", true, "SELECT 2 AS `a` EXCEPT SELECT 1 AS `b` ORDER BY `a`"},
		{"table t1 except table t2", true, "TABLE `t1` EXCEPT TABLE `t2`"},
		{"table t1 except (table t2)", true, "TABLE `t1` EXCEPT (TABLE `t2`)"},
		{"table t1 except select * from t2", true, "TABLE `t1` EXCEPT SELECT * FROM `t2`"},
		{"select * from t1 except table t2", true, "SELECT * FROM `t1` EXCEPT TABLE `t2`"},
		{"table t1 except (select c2 from t2) order by c1 limit 1", true, "TABLE `t1` EXCEPT (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"},
		{"select c1 from t1 except (table t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` EXCEPT (TABLE `t2`) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) except table t2 except (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) EXCEPT TABLE `t2` EXCEPT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"(table t1) except select c2 from t2 except (table t3) order by c1 limit 1", true, "(TABLE `t1`) EXCEPT SELECT `c2` FROM `t2` EXCEPT (TABLE `t3`) ORDER BY `c1` LIMIT 1"},
		{"values row(1,-2,3), row(5,7,9) except values row(1,-2,3), row(5,7,9)", true, "VALUES ROW(1,-2,3), ROW(5,7,9) EXCEPT VALUES ROW(1,-2,3), ROW(5,7,9)"},
		{"values row(1,-2,3), row(5,7,9) except (values row(1,-2,3), row(5,7,9))", true, "VALUES ROW(1,-2,3), ROW(5,7,9) EXCEPT (VALUES ROW(1,-2,3), ROW(5,7,9))"},
		{"values row(1,-2,3), row(5,7,9) except select * from t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) EXCEPT SELECT * FROM `t`"},
		{"values row(1,-2,3), row(5,7,9) except table t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) EXCEPT TABLE `t`"},
		{"select * from t except values row(1,-2,3), row(5,7,9)", true, "SELECT * FROM `t` EXCEPT VALUES ROW(1,-2,3), ROW(5,7,9)"},
		{"table t except values row(1,-2,3), row(5,7,9)", true, "TABLE `t` EXCEPT VALUES ROW(1,-2,3), ROW(5,7,9)"},
		// intersect
		{"select c1 from t1 intersect select c2 from t2", true, "SELECT `c1` FROM `t1` INTERSECT SELECT `c2` FROM `t2`"},
		{"select c1 from t1 intersect (select c2 from t2)", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`)"},
		{"select c1 from t1 intersect (select c2 from t2) order by c1", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`) ORDER BY `c1`"},
		{"select c1 from t1 intersect select c2 from t2 order by c2", true, "SELECT `c1` FROM `t1` INTERSECT SELECT `c2` FROM `t2` ORDER BY `c2`"},
		{"select c1 from t1 intersect (select c2 from t2) limit 1", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`) LIMIT 1"},
		{"select c1 from t1 intersect (select c2 from t2) limit 1, 1", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`) LIMIT 1,1"},
		{"select c1 from t1 intersect (select c2 from t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` INTERSECT (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) intersect (select c2 from t2) order by c1 intersect select c3 from t3", false, ""},
		{"(select c1 from t1) intersect (select c2 from t2) limit 1 intersect select c3 from t3", false, ""},
		{"(select c1 from t1) intersect select c2 from t2 intersect (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) INTERSECT SELECT `c2` FROM `t2` INTERSECT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"select (select 1 intersect select 1) as a", true, "SELECT (SELECT 1 INTERSECT SELECT 1) AS `a`"},
		{"select * from (select 1 intersect select 2) as a", true, "SELECT * FROM (SELECT 1 INTERSECT SELECT 2) AS `a`"},
		{"insert into t select c1 from t1 intersect select c2 from t2", true, "INSERT INTO `t` SELECT `c1` FROM `t1` INTERSECT SELECT `c2` FROM `t2`"},
		{"insert into t (c) select c1 from t1 intersect select c2 from t2", true, "INSERT INTO `t` (`c`) SELECT `c1` FROM `t1` INTERSECT SELECT `c2` FROM `t2`"},
		{"select 2 as a from dual intersect select 1 as b from dual order by a", true, "SELECT 2 AS `a` INTERSECT SELECT 1 AS `b` ORDER BY `a`"},
		{"table t1 intersect table t2", true, "TABLE `t1` INTERSECT TABLE `t2`"},
		{"table t1 intersect (table t2)", true, "TABLE `t1` INTERSECT (TABLE `t2`)"},
		{"table t1 intersect select * from t2", true, "TABLE `t1` INTERSECT SELECT * FROM `t2`"},
		{"select * from t1 intersect table t2", true, "SELECT * FROM `t1` INTERSECT TABLE `t2`"},
		{"table t1 intersect (select c2 from t2) order by c1 limit 1", true, "TABLE `t1` INTERSECT (SELECT `c2` FROM `t2`) ORDER BY `c1` LIMIT 1"},
		{"select c1 from t1 intersect (table t2) order by c1 limit 1", true, "SELECT `c1` FROM `t1` INTERSECT (TABLE `t2`) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) intersect table t2 intersect (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) INTERSECT TABLE `t2` INTERSECT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"(table t1) intersect select c2 from t2 intersect (table t3) order by c1 limit 1", true, "(TABLE `t1`) INTERSECT SELECT `c2` FROM `t2` INTERSECT (TABLE `t3`) ORDER BY `c1` LIMIT 1"},
		{"values row(1,-2,3), row(5,7,9) intersect values row(1,-2,3), row(5,7,9)", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTERSECT VALUES ROW(1,-2,3), ROW(5,7,9)"},
		{"values row(1,-2,3), row(5,7,9) intersect (values row(1,-2,3), row(5,7,9))", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTERSECT (VALUES ROW(1,-2,3), ROW(5,7,9))"},
		{"values row(1,-2,3), row(5,7,9) intersect select * from t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTERSECT SELECT * FROM `t`"},
		{"values row(1,-2,3), row(5,7,9) intersect table t", true, "VALUES ROW(1,-2,3), ROW(5,7,9) INTERSECT TABLE `t`"},
		{"select * from t intersect values row(1,-2,3), row(5,7,9)", true, "SELECT * FROM `t` INTERSECT VALUES ROW(1,-2,3), ROW(5,7,9)"},
		{"table t intersect values row(1,-2,3), row(5,7,9)", true, "TABLE `t` INTERSECT VALUES ROW(1,-2,3), ROW(5,7,9)"},
		// mixture of union, except and intersect
		{"(select c1 from t1) intersect select c2 from t2 union (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) INTERSECT SELECT `c2` FROM `t2` UNION (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) union all select c2 from t2 except (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) UNION ALL SELECT `c2` FROM `t2` EXCEPT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) except select c2 from t2 intersect (select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) EXCEPT SELECT `c2` FROM `t2` INTERSECT (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"select 1 union distinct select 1 except select 1 intersect select 1", true, "SELECT 1 UNION SELECT 1 EXCEPT SELECT 1 INTERSECT SELECT 1"},
		// mixture of union, except and intersect with parentheses
		{"(select c1 from t1) intersect all (select c2 from t2 union (select c3 from t3)) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) INTERSECT ALL (SELECT `c2` FROM `t2` UNION (SELECT `c3` FROM `t3`)) ORDER BY `c1` LIMIT 1"},
		{"(select c1 from t1) union all (select c2 from t2 except select c3 from t3) order by c1 limit 1", true, "(SELECT `c1` FROM `t1`) UNION ALL (SELECT `c2` FROM `t2` EXCEPT SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"((select c1 from t1) except select c2 from t2) intersect all (select c3 from t3) order by c1 limit 1", true, "((SELECT `c1` FROM `t1`) EXCEPT SELECT `c2` FROM `t2`) INTERSECT ALL (SELECT `c3` FROM `t3`) ORDER BY `c1` LIMIT 1"},
		{"select 1 union distinct (select 1 except all select 1 intersect select 1)", true, "SELECT 1 UNION (SELECT 1 EXCEPT ALL SELECT 1 INTERSECT SELECT 1)"},
		// https://github.com/pingcap/tidb/issues/49874
		{"select * from a where PK = 0 union all (select * from b where PK = 0 union all (select * from b where PK != 0) order by pk limit 1)", true,
			"SELECT * FROM `a` WHERE `PK`=0 UNION ALL (SELECT * FROM `b` WHERE `PK`=0 UNION ALL (SELECT * FROM `b` WHERE `PK`!=0) ORDER BY `pk` LIMIT 1)"},
		{"select * from a where PK = 0 union all (select * from b where PK = 0 union all (select * from b where PK != 0) order by pk limit 1) order by pk limit 2", true,
			"SELECT * FROM `a` WHERE `PK`=0 UNION ALL (SELECT * FROM `b` WHERE `PK`=0 UNION ALL (SELECT * FROM `b` WHERE `PK`!=0) ORDER BY `pk` LIMIT 1) ORDER BY `pk` LIMIT 2"},
		{"(select * from b where pk= 0 union all (select * from b where pk !=0) order by pk limit 1) order by pk limit 2", true,
			"(SELECT * FROM `b` WHERE `pk`=0 UNION ALL (SELECT * FROM `b` WHERE `pk`!=0) ORDER BY `pk` LIMIT 1) ORDER BY `pk` LIMIT 2"},
		{"(select * from b where pk= 0 union all (select * from b where pk !=0) order by pk limit 1) order by pk", true,
			"(SELECT * FROM `b` WHERE `pk`=0 UNION ALL (SELECT * FROM `b` WHERE `pk`!=0) ORDER BY `pk` LIMIT 1) ORDER BY `pk`"},
	}
	s.RunTest(c, table, false)
}

func checkOrderBy(c *C, s ast.Node, hasOrderBy []bool, i int) int {
	switch x := s.(type) {
	case *ast.SelectStmt:
		c.Assert(x.OrderBy != nil, Equals, hasOrderBy[i])
		return i + 1
	case *ast.SetOprSelectList:
		for _, sel := range x.Selects {
			i = checkOrderBy(c, sel, hasOrderBy, i)
		}
		return i
	}
	return i
}

func (s *testParserSuite) TestLikeEscape(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// for like escape
		{`select "abc_" like "abc\\_" escape ''`, true, "SELECT 'abc_' LIKE 'abc\\_'"},
		{`select "abc_" like "abc\\_" escape '\\'`, true, "SELECT 'abc_' LIKE 'abc\\_'"},
		{`select "abc_" like "abc\\_" escape '||'`, false, ""},
		{`select "abc" like "escape" escape '+'`, true, "SELECT 'abc' LIKE 'escape' ESCAPE '+'"},
		{"select '''_' like '''_' escape ''''", true, "SELECT '''_' LIKE '''_' ESCAPE ''''"},
	}

	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestMysqlDump(c *C) {
	defer testleak.AfterTest(c)()
	// Statements used by mysqldump.
	table := []testCase{
		{`UNLOCK TABLES;`, true, ""},
		{`LOCK TABLES t1 READ;`, true, ""},
		{`show table status like 't'`, true, "SHOW TABLE STATUS LIKE 't'"},
		{`LOCK TABLES t2 WRITE`, true, ""},

		// for unlock table and lock table
		{`UNLOCK TABLE;`, true, ""},
		{`LOCK TABLE t1 READ;`, true, ""},
		{`show table status like 't'`, true, "SHOW TABLE STATUS LIKE 't'"},
		{`LOCK TABLE t2 WRITE`, true, ""},
		{`LOCK TABLE t1 WRITE, t3 READ`, true, ""},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestIndexHint(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{`select * from t use index (primary)`, true, "SELECT * FROM `t` USE INDEX (`primary`)"},
		{"select * from t use index (`primary`)", true, "SELECT * FROM `t` USE INDEX (`primary`)"},
		{`select * from t use index ();`, true, "SELECT * FROM `t` USE INDEX ()"},
		{`select * from t use index (idx);`, true, "SELECT * FROM `t` USE INDEX (`idx`)"},
		{`select * from t use index (idx1, idx2);`, true, "SELECT * FROM `t` USE INDEX (`idx1`, `idx2`)"},
		{`select * from t ignore key (idx1)`, true, "SELECT * FROM `t` IGNORE INDEX (`idx1`)"},
		{`select * from t force index for join (idx1)`, true, "SELECT * FROM `t` FORCE INDEX FOR JOIN (`idx1`)"},
		{`select * from t use index for order by (idx1)`, true, "SELECT * FROM `t` USE INDEX FOR ORDER BY (`idx1`)"},
		{`select * from t force index for group by (idx1)`, true, "SELECT * FROM `t` FORCE INDEX FOR GROUP BY (`idx1`)"},
		{`select * from t use index for group by (idx1) use index for order by (idx2), t2`, true, "SELECT * FROM (`t` USE INDEX FOR GROUP BY (`idx1`) USE INDEX FOR ORDER BY (`idx2`)) JOIN `t2`"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestPriority(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{`select high_priority * from t`, true, "SELECT HIGH_PRIORITY * FROM `t`"},
		{`select low_priority * from t`, true, "SELECT LOW_PRIORITY * FROM `t`"},
		{`select delayed * from t`, true, "SELECT DELAYED * FROM `t`"},
		{`insert high_priority into t values (1)`, true, "INSERT HIGH_PRIORITY INTO `t` VALUES (1)"},
		{`insert LOW_PRIORITY into t values (1)`, true, "INSERT LOW_PRIORITY INTO `t` VALUES (1)"},
		{`insert delayed into t values (1)`, true, "INSERT DELAYED INTO `t` VALUES (1)"},
		{`update low_priority t set a = 2`, true, "UPDATE LOW_PRIORITY `t` SET `a`=2"},
		{`update high_priority t set a = 2`, true, "UPDATE HIGH_PRIORITY `t` SET `a`=2"},
		{`update delayed t set a = 2`, true, "UPDATE DELAYED `t` SET `a`=2"},
		{`delete low_priority from t where a = 2`, true, "DELETE LOW_PRIORITY FROM `t` WHERE `a`=2"},
		{`delete high_priority from t where a = 2`, true, "DELETE HIGH_PRIORITY FROM `t` WHERE `a`=2"},
		{`delete delayed from t where a = 2`, true, "DELETE DELAYED FROM `t` WHERE `a`=2"},
		{`replace high_priority into t values (1)`, true, "REPLACE HIGH_PRIORITY INTO `t` VALUES (1)"},
		{`replace LOW_PRIORITY into t values (1)`, true, "REPLACE LOW_PRIORITY INTO `t` VALUES (1)"},
		{`replace delayed into t values (1)`, true, "REPLACE DELAYED INTO `t` VALUES (1)"},
	}
	s.RunTest(c, table, false)

	parser := New()
	stmt, _, err := parser.Parse("select HIGH_PRIORITY * from t", "", "")
	c.Assert(err, IsNil)
	sel := stmt[0].(*ast.SelectStmt)
	c.Assert(sel.SelectStmtOpts.Priority, Equals, mysql.HighPriority)
}

func (s *testParserSuite) TestSQLNoCache(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{`select SQL_NO_CACHE * from t`, false, ""},
		{`select SQL_CACHE * from t`, true, "SELECT * FROM `t`"},
		{`select * from t`, true, "SELECT * FROM `t`"},
	}

	parser := New()
	for _, tt := range table {
		stmt, _, err := parser.Parse(tt.src, "", "")
		c.Assert(err, IsNil)

		sel := stmt[0].(*ast.SelectStmt)
		c.Assert(sel.SelectStmtOpts.SQLCache, Equals, tt.ok)
	}
}

func (s *testParserSuite) TestEscape(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{`select """;`, false, ""},
		{`select """";`, true, "SELECT '\"'"},
		{`select "汉字";`, true, "SELECT '汉字'"},
		{`select 'abc"def';`, true, "SELECT 'abc\"def'"},
		{`select 'a\r\n';`, true, "SELECT 'a\r\n'"},
		{`select "\a\r\n"`, true, "SELECT 'a\r\n'"},
		{`select "\xFF"`, true, "SELECT 'xFF'"},
	}
	s.RunTest(c, table, false)
}
func (s *testParserSuite) TestWindowFunctions(c *C) {
	table := []testCase{
		// For window function descriptions.
		// See https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html
		{`SELECT CUME_DIST() OVER w FROM t;`, true, "SELECT CUME_DIST() OVER `w` FROM `t`"},
		{`SELECT DENSE_RANK() OVER (w) FROM t;`, true, "SELECT DENSE_RANK() OVER (`w`) FROM `t`"},
		{`SELECT FIRST_VALUE(val) OVER w FROM t;`, true, "SELECT FIRST_VALUE(`val`) OVER `w` FROM `t`"},
		{`SELECT FIRST_VALUE(val) RESPECT NULLS OVER w FROM t;`, true, "SELECT FIRST_VALUE(`val`) OVER `w` FROM `t`"},
		{`SELECT FIRST_VALUE(val) IGNORE NULLS OVER w FROM t;`, true, "SELECT FIRST_VALUE(`val`) IGNORE NULLS OVER `w` FROM `t`"},
		{`SELECT LAG(val) OVER (w) FROM t;`, true, "SELECT LAG(`val`) OVER (`w`) FROM `t`"},
		{`SELECT LAG(val, 1) OVER (w) FROM t;`, true, "SELECT LAG(`val`, 1) OVER (`w`) FROM `t`"},
		{`SELECT LAG(val, 1, def) OVER (w) FROM t;`, true, "SELECT LAG(`val`, 1, `def`) OVER (`w`) FROM `t`"},
		{`SELECT LAST_VALUE(val) OVER (w) FROM t;`, true, "SELECT LAST_VALUE(`val`) OVER (`w`) FROM `t`"},
		{`SELECT LEAD(val) OVER w FROM t;`, true, "SELECT LEAD(`val`) OVER `w` FROM `t`"},
		{`SELECT LEAD(val, 1) OVER w FROM t;`, true, "SELECT LEAD(`val`, 1) OVER `w` FROM `t`"},
		{`SELECT LEAD(val, 1, def) OVER w FROM t;`, true, "SELECT LEAD(`val`, 1, `def`) OVER `w` FROM `t`"},
		{`SELECT NTH_VALUE(val, 233) OVER w FROM t;`, true, "SELECT NTH_VALUE(`val`, 233) OVER `w` FROM `t`"},
		{`SELECT NTH_VALUE(val, 233) FROM FIRST OVER w FROM t;`, true, "SELECT NTH_VALUE(`val`, 233) OVER `w` FROM `t`"},
		{`SELECT NTH_VALUE(val, 233) FROM LAST OVER w FROM t;`, true, "SELECT NTH_VALUE(`val`, 233) FROM LAST OVER `w` FROM `t`"},
		{`SELECT NTH_VALUE(val, 233) FROM LAST IGNORE NULLS OVER w FROM t;`, true, "SELECT NTH_VALUE(`val`, 233) FROM LAST IGNORE NULLS OVER `w` FROM `t`"},
		{`SELECT NTH_VALUE(val) OVER w FROM t;`, false, ""},
		{`SELECT NTILE(233) OVER (w) FROM t;`, true, "SELECT NTILE(233) OVER (`w`) FROM `t`"},
		{`SELECT PERCENT_RANK() OVER (w) FROM t;`, true, "SELECT PERCENT_RANK() OVER (`w`) FROM `t`"},
		{`SELECT RANK() OVER (w) FROM t;`, true, "SELECT RANK() OVER (`w`) FROM `t`"},
		{`SELECT ROW_NUMBER() OVER (w) FROM t;`, true, "SELECT ROW_NUMBER() OVER (`w`) FROM `t`"},
		{`SELECT n, LAG(n, 1, 0) OVER (w), LEAD(n, 1, 0) OVER w, n + LAG(n, 1, 0) OVER (w) FROM fib;`, true, "SELECT `n`,LAG(`n`, 1, 0) OVER (`w`),LEAD(`n`, 1, 0) OVER `w`,`n`+LAG(`n`, 1, 0) OVER (`w`) FROM `fib`"},

		// For window function concepts and syntax.
		// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html
		{`SELECT SUM(profit) OVER(PARTITION BY country) AS country_profit FROM sales;`, true, "SELECT SUM(`profit`) OVER (PARTITION BY `country`) AS `country_profit` FROM `sales`"},
		{`SELECT SUM(profit) OVER() AS country_profit FROM sales;`, true, "SELECT SUM(`profit`) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT AVG(profit) OVER() AS country_profit FROM sales;`, true, "SELECT AVG(`profit`) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT BIT_XOR(profit) OVER() AS country_profit FROM sales;`, true, "SELECT BIT_XOR(`profit`) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT COUNT(profit) OVER() AS country_profit FROM sales;`, true, "SELECT COUNT(`profit`) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT COUNT(ALL profit) OVER() AS country_profit FROM sales;`, true, "SELECT COUNT(`profit`) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT COUNT(*) OVER() AS country_profit FROM sales;`, true, "SELECT COUNT(1) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT MAX(profit) OVER() AS country_profit FROM sales;`, true, "SELECT MAX(`profit`) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT MIN(profit) OVER() AS country_profit FROM sales;`, true, "SELECT MIN(`profit`) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT SUM(profit) OVER() AS country_profit FROM sales;`, true, "SELECT SUM(`profit`) OVER () AS `country_profit` FROM `sales`"},
		{`SELECT ROW_NUMBER() OVER(PARTITION BY country) AS row_num1 FROM sales;`, true, "SELECT ROW_NUMBER() OVER (PARTITION BY `country`) AS `row_num1` FROM `sales`"},
		{`SELECT ROW_NUMBER() OVER(PARTITION BY country, d ORDER BY year, product) AS row_num2 FROM sales;`, true, "SELECT ROW_NUMBER() OVER (PARTITION BY `country`, `d` ORDER BY `year`,`product`) AS `row_num2` FROM `sales`"},

		// For window function frame specification.
		// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
		{`SELECT SUM(val) OVER (PARTITION BY subject ORDER BY time ROWS UNBOUNDED PRECEDING) FROM t;`, true, "SELECT SUM(`val`) OVER (PARTITION BY `subject` ORDER BY `time` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM `t`"},
		{`SELECT AVG(val) OVER (PARTITION BY subject ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;`, true, "SELECT AVG(`val`) OVER (PARTITION BY `subject` ORDER BY `time` ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM `t`"},
		{`SELECT AVG(val) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;`, true, "SELECT AVG(`val`) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM `t`"},
		{`SELECT AVG(val) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM t;`, true, "SELECT AVG(`val`) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM `t`"},
		{`SELECT AVG(val) OVER (RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL '2:30' MINUTE_SECOND FOLLOWING) FROM t;`, true, "SELECT AVG(`val`) OVER (RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL _UTF8MB4'2:30' MINUTE_SECOND FOLLOWING) FROM `t`"},
		{`SELECT AVG(val) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM t;`, true, "SELECT AVG(`val`) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM `t`"},
		{`SELECT AVG(val) OVER (RANGE CURRENT ROW) FROM t;`, true, "SELECT AVG(`val`) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM `t`"},

		// For named windows.
		// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-named-windows.html
		{`SELECT RANK() OVER (w) FROM t WINDOW w AS (ORDER BY val);`, true, "SELECT RANK() OVER (`w`) FROM `t` WINDOW `w` AS (ORDER BY `val`)"},
		{`SELECT RANK() OVER w FROM t WINDOW w AS ();`, true, "SELECT RANK() OVER `w` FROM `t` WINDOW `w` AS ()"},
		{`SELECT FIRST_VALUE(year) OVER (w ORDER BY year) AS first FROM sales WINDOW w AS (PARTITION BY country);`, true, "SELECT FIRST_VALUE(`year`) OVER (`w` ORDER BY `year`) AS `first` FROM `sales` WINDOW `w` AS (PARTITION BY `country`)"},
		{`SELECT RANK() OVER (w1) FROM t WINDOW w1 AS (w2), w2 AS (), w3 AS (w1);`, true, "SELECT RANK() OVER (`w1`) FROM `t` WINDOW `w1` AS (`w2`),`w2` AS (),`w3` AS (`w1`)"},
		{`SELECT RANK() OVER w1 FROM t WINDOW w1 AS (w2), w2 AS (w3), w3 AS (w1);`, true, "SELECT RANK() OVER `w1` FROM `t` WINDOW `w1` AS (`w2`),`w2` AS (`w3`),`w3` AS (`w1`)"},
	}
	s.RunTest(c, table, true)
}

func (s *testParserSuite) TestInsertStatementMemoryAllocation(c *C) {
	sql := "insert t values (1)" + strings.Repeat(",(1)", 1000)
	var oldStats, newStats runtime.MemStats
	runtime.ReadMemStats(&oldStats)
	_, err := New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	runtime.ReadMemStats(&newStats)
	c.Assert(int(newStats.TotalAlloc-oldStats.TotalAlloc), Less, 1024*500)
}

func (s *testParserSuite) TestExplain(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{"explain select c1 from t1", true, "EXPLAIN FORMAT = 'row' SELECT `c1` FROM `t1`"},
		{"explain delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;", true, "EXPLAIN FORMAT = 'row' DELETE `t1`,`t2` FROM (`t1` JOIN `t2`) JOIN `t3` WHERE `t1`.`id`=`t2`.`id` AND `t2`.`id`=`t3`.`id`"},
		{"explain insert into t values (1), (2), (3)", true, "EXPLAIN FORMAT = 'row' INSERT INTO `t` VALUES (1),(2),(3)"},
		{"explain replace into foo values (1 || 2)", true, "EXPLAIN FORMAT = 'row' REPLACE INTO `foo` VALUES (1 OR 2)"},
		{"explain update t set id = id + 1 order by id desc;", true, "EXPLAIN FORMAT = 'row' UPDATE `t` SET `id`=`id`+1 ORDER BY `id` DESC"},
		{"explain select c1 from t1 union (select c2 from t2) limit 1, 1", true, "EXPLAIN FORMAT = 'row' SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) LIMIT 1,1"},
		{`explain format = "row" select c1 from t1 union (select c2 from t2) limit 1, 1`, true, "EXPLAIN FORMAT = 'row' SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) LIMIT 1,1"},
		{"DESC SCHE.TABL", true, "DESC `SCHE`.`TABL`"},
		{"DESC SCHE.TABL COLUM", true, "DESC `SCHE`.`TABL` `COLUM`"},
		{"DESCRIBE SCHE.TABL COLUM", true, "DESC `SCHE`.`TABL` `COLUM`"},
		{"EXPLAIN ANALYZE SELECT 1", true, "EXPLAIN ANALYZE SELECT 1"},
		{"EXPLAIN FORMAT = 'dot' SELECT 1", true, "EXPLAIN FORMAT = 'dot' SELECT 1"},
		{"EXPLAIN FORMAT = 'row' SELECT 1", true, "EXPLAIN FORMAT = 'row' SELECT 1"},
		{"EXPLAIN FORMAT = 'ROW' SELECT 1", true, "EXPLAIN FORMAT = 'ROW' SELECT 1"},
		{"EXPLAIN SELECT 1", true, "EXPLAIN FORMAT = 'row' SELECT 1"},
		// {"EXPLAIN FOR CONNECTION 1", true, "EXPLAIN FORMAT = 'row' FOR CONNECTION 1"},
		// {"EXPLAIN FOR connection 42", true, "EXPLAIN FORMAT = 'row' FOR CONNECTION 42"},
		// {"EXPLAIN FORMAT = 'dot' FOR CONNECTION 1", true, "EXPLAIN FORMAT = 'dot' FOR CONNECTION 1"},
		// {"EXPLAIN FORMAT = 'row' FOR connection 1", true, "EXPLAIN FORMAT = 'row' FOR CONNECTION 1"},
		// {"EXPLAIN FORMAT = TRADITIONAL FOR CONNECTION 1", true, "EXPLAIN FORMAT = 'row' FOR CONNECTION 1"},
		// {"EXPLAIN FORMAT = TRADITIONAL SELECT 1", true, "EXPLAIN FORMAT = 'row' SELECT 1"},
		// {"EXPLAIN FORMAT = JSON FOR CONNECTION 1", true, "EXPLAIN FORMAT = 'json' FOR CONNECTION 1"},
		// {"EXPLAIN FORMAT = JSON SELECT 1", true, "EXPLAIN FORMAT = 'json' SELECT 1"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestPrepare(c *C) {
	table := []testCase{
		{"PREPARE pname FROM 'SELECT ?'", true, "PREPARE `pname` FROM 'SELECT ?'"},
		{"PREPARE pname FROM @test", true, "PREPARE `pname` FROM @`test`"},
		{"PREPARE `` FROM @test", true, "PREPARE `` FROM @`test`"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestDeallocate(c *C) {
	table := []testCase{
		{"DEALLOCATE PREPARE test", true, "DEALLOCATE PREPARE `test`"},
		{"DEALLOCATE PREPARE ``", true, "DEALLOCATE PREPARE ``"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestExecute(c *C) {
	table := []testCase{
		{"EXECUTE test", true, "EXECUTE `test`"},
		{"EXECUTE test USING @var1,@var2", true, "EXECUTE `test` USING @`var1`,@`var2`"},
		{"EXECUTE `` USING @var1,@var2", true, "EXECUTE `` USING @`var1`,@`var2`"},
	}
	s.RunTest(c, table, false)
}

// func (s *testParserSuite) TestTrace(c *C) {
// 	defer testleak.AfterTest(c)()
// 	table := []testCase{
// 		{"trace select c1 from t1", true, "TRACE SELECT `c1` FROM `t1`"},
// 		{"trace delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;", true, "TRACE DELETE `t1`,`t2` FROM (`t1` JOIN `t2`) JOIN `t3` WHERE `t1`.`id`=`t2`.`id` AND `t2`.`id`=`t3`.`id`"},
// 		{"trace insert into t values (1), (2), (3)", true, "TRACE INSERT INTO `t` VALUES (1),(2),(3)"},
// 		{"trace replace into foo values (1 || 2)", true, "TRACE REPLACE INTO `foo` VALUES (1 OR 2)"},
// 		{"trace update t set id = id + 1 order by id desc;", true, "TRACE UPDATE `t` SET `id`=`id`+1 ORDER BY `id` DESC"},
// 		{"trace select c1 from t1 union (select c2 from t2) limit 1, 1", true, "TRACE SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) LIMIT 1,1"},
// 		{"trace format = 'row' select c1 from t1 union (select c2 from t2) limit 1, 1", true, "TRACE FORMAT = 'row' SELECT `c1` FROM `t1` UNION (SELECT `c2` FROM `t2`) LIMIT 1,1"},
// 		{"trace format = 'json' update t set id = id + 1 order by id desc;", true, "TRACE UPDATE `t` SET `id`=`id`+1 ORDER BY `id` DESC"},
// 	}
// 	s.RunTest(c, table)
// }

func (s *testParserSuite) TestView(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{"create view v as select * from t", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"create or replace view v as select * from t", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"create or replace algorithm = undefined view v as select * from t", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"create or replace algorithm = merge view v as select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"create or replace algorithm = temptable view v as select * from t", true, "CREATE OR REPLACE ALGORITHM = TEMPTABLE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"create or replace algorithm = merge definer = 'root' view v as select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"create or replace algorithm = merge definer = 'root' sql security definer view v as select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v as select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` AS SELECT * FROM `t`"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t`"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t with local check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t` WITH LOCAL CHECK OPTION"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t with cascaded check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t`"},
		{"create or replace algorithm = merge definer = current_user view v as select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t`"},

		// create view with `(` select statement `)`
		{"create view v as (select * from t)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t`)"},
		{"create or replace view v as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = undefined view v as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = merge view v as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = temptable view v as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = TEMPTABLE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' view v as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' sql security definer view v as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t) with local check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS (SELECT * FROM `t`) WITH LOCAL CHECK OPTION"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t) with cascaded check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS (SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = current_user view v as (select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t`)"},

		// create view with union statement
		{"create view v as (select * from t union all select * from t)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = undefined view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = merge view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = temptable view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = TEMPTABLE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' sql security definer view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t union all select * from t) with local check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`) WITH LOCAL CHECK OPTION"},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t union all select * from t) with cascaded check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS (SELECT * FROM `t` UNION ALL SELECT * FROM `t`)"},
		{"create or replace algorithm = merge definer = current_user view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},

		// create view with union all statement
		// {"create view v as select * from t union all select * from t", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = undefined view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = temptable view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = TEMPTABLE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' sql security definer view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' sql security invoker view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t union all select * from t with local check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t` UNION ALL SELECT * FROM `t` WITH LOCAL CHECK OPTION"},
		// {"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t union all select * from t with cascaded check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = current_user view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},

		// create view with `(` union statement `)`
		// {"create view v as (select * from t union all select * from t)", true, "CREATE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = undefined view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = temptable view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = TEMPTABLE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' sql security definer view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' sql security invoker view v as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t union all select * from t)", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t union all select * from t) with local check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t` UNION ALL SELECT * FROM `t` WITH LOCAL CHECK OPTION"},
		// {"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as (select * from t union all select * from t) with cascaded check option", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = `root`@`%` SQL SECURITY INVOKER VIEW `v` (`a`,`b`) AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
		// {"create or replace algorithm = merge definer = current_user view v as select * from t union all select * from t", true, "CREATE OR REPLACE ALGORITHM = MERGE DEFINER = CURRENT_USER SQL SECURITY DEFINER VIEW `v` AS SELECT * FROM `t` UNION ALL SELECT * FROM `t`"},
	}
	s.RunTest(c, table, false)

	// Test case for the text of the select statement in create view statement.
	p := New()
	sms, _, err := p.Parse("create view v as select * from t", "", "")
	c.Assert(err, IsNil)
	v, ok := sms[0].(*ast.CreateViewStmt)
	c.Assert(ok, IsTrue)
	c.Assert(v.Algorithm, Equals, model.AlgorithmUndefined)
	c.Assert(v.Select.Text(), Equals, "select * from t")
	c.Assert(v.Security, Equals, model.SecurityDefiner)
	// c.Assert(v.CheckOption, Equals, model.CheckOptionCascaded)

	// src := `CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = root@localhost
	//               SQL SECURITY DEFINER
	// 		      VIEW V(a,b,c) AS select c,d,e from t
	//               WITH CASCADED CHECK OPTION;`

	// var st ast.StmtNode
	// st, err = p.ParseOneStmt(src, "", "")
	// c.Assert(err, IsNil)
	// v, ok = st.(*ast.CreateViewStmt)
	// c.Assert(ok, IsTrue)
	// c.Assert(v.OrReplace, IsTrue)
	// c.Assert(v.Algorithm, Equals, model.AlgorithmUndefined)
	// c.Assert(v.Definer.Username, Equals, "root")
	// c.Assert(v.Definer.Hostname, Equals, "localhost")
	// c.Assert(v.Cols[0], Equals, model.NewCIStr("a"))
	// c.Assert(v.Cols[1], Equals, model.NewCIStr("b"))
	// c.Assert(v.Cols[2], Equals, model.NewCIStr("c"))
	// c.Assert(v.Select.Text(), Equals, "select c,d,e from t")
	// c.Assert(v.Security, Equals, model.SecurityDefiner)
	// c.Assert(v.CheckOption, Equals, model.CheckOptionCascaded)
}

func (s *testParserSuite) TestTimestampDiffUnit(c *C) {
	// Test case for timestampdiff unit.
	// TimeUnit should be unified to upper case.
	parser := New()
	stmt, _, err := parser.Parse("SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01'), TIMESTAMPDIFF(month,'2003-02-01','2003-05-01');", "", "")
	c.Assert(err, IsNil)
	ss := stmt[0].(*ast.SelectStmt)
	fields := ss.Fields.Fields
	c.Assert(len(fields), Equals, 2)
	expr := fields[0].Expr
	f, ok := expr.(*ast.FuncCallExpr)
	c.Assert(ok, IsTrue)
	c.Assert(f.Args[0].(*ast.TimeUnitExpr).Unit, Equals, ast.TimeUnitMonth)

	expr = fields[1].Expr
	f, ok = expr.(*ast.FuncCallExpr)
	c.Assert(ok, IsTrue)
	c.Assert(f.Args[0].(*ast.TimeUnitExpr).Unit, Equals, ast.TimeUnitMonth)

	// Test Illegal TimeUnit for TimestampDiff
	table := []testCase{
		{"SELECT TIMESTAMPDIFF(SECOND_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(MINUTE_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(MINUTE_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_MINUTE,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_MINUTE,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_HOUR,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(YEAR_MONTH,'2003-02-01','2003-05-01')", false, ""},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestSessionManage(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// Kill statement.
		// See https://dev.mysql.com/doc/refman/5.7/en/kill.html
		{"kill 23123", true, "KILL 23123"},
		{"kill connection 23123", true, "KILL 23123"},
		{"kill query 23123", true, "KILL QUERY 23123"},
		{"kill tidb 23123", true, "KILL TIDB 23123"},
		{"kill tidb connection 23123", true, "KILL TIDB 23123"},
		{"kill tidb query 23123", true, "KILL TIDB QUERY 23123"},
		{"show processlist", true, "SHOW PROCESSLIST"},
		{"show full processlist", true, "SHOW FULL PROCESSLIST"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestSQLModeANSIQuotes(c *C) {
	parser := New()
	parser.SetSQLMode(mysql.ModeANSIQuotes)
	tests := []string{
		`CREATE TABLE "table" ("id" int)`,
		`select * from t "tt"`,
	}
	for _, test := range tests {
		_, _, err := parser.Parse(test, "", "")
		c.Assert(err, IsNil)
	}
}

func (s *testParserSuite) TestDDLStatements(c *C) {
	parser := New()
	// Tests that whatever the charset it is define, we always assign utf8 charset and utf8_bin collate.
	createTableStr := `CREATE TABLE t (
		a varchar(64) binary,
		b char(10) charset utf8 collate utf8_general_ci,
		c text charset latin1) ENGINE=innoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin`
	stmts, _, err := parser.Parse(createTableStr, "", "")
	c.Assert(err, IsNil)
	stmt := stmts[0].(*ast.CreateTableStmt)
	c.Assert(mysql.HasBinaryFlag(stmt.Cols[0].Tp.Flag), IsTrue)
	for _, colDef := range stmt.Cols[1:] {
		c.Assert(mysql.HasBinaryFlag(colDef.Tp.Flag), IsFalse)
	}
	for _, tblOpt := range stmt.Options {
		switch tblOpt.Tp {
		case ast.TableOptionCharset:
			c.Assert(tblOpt.StrValue, Equals, "utf8")
		case ast.TableOptionCollate:
			c.Assert(tblOpt.StrValue, Equals, "utf8_bin")
		}
	}
	createTableStr = `CREATE TABLE t (
		a varbinary(64),
		b binary(10),
		c blob)`
	stmts, _, err = parser.Parse(createTableStr, "", "")
	c.Assert(err, IsNil)
	stmt = stmts[0].(*ast.CreateTableStmt)
	for _, colDef := range stmt.Cols {
		c.Assert(colDef.Tp.Charset, Equals, charset.CharsetBin)
		c.Assert(colDef.Tp.Collate, Equals, charset.CollationBin)
		c.Assert(mysql.HasBinaryFlag(colDef.Tp.Flag), IsTrue)
	}
	// Test set collate for all column types
	createTableStr = `CREATE TABLE t (
		c_int int collate utf8_bin,
		c_real real collate utf8_bin,
		c_float float collate utf8_bin,
		c_bool bool collate utf8_bin,
		c_char char collate utf8_bin,
		c_binary binary collate utf8_bin,
		c_varchar varchar(2) collate utf8_bin,
		c_year year collate utf8_bin,
		c_date date collate utf8_bin,
		c_time time collate utf8_bin,
		c_datetime datetime collate utf8_bin,
		c_timestamp timestamp collate utf8_bin,
		c_tinyblob tinyblob collate utf8_bin,
		c_blob blob collate utf8_bin,
		c_mediumblob mediumblob collate utf8_bin,
		c_longblob longblob collate utf8_bin,
		c_bit bit collate utf8_bin,
		c_long_varchar long varchar collate utf8_bin,
		c_tinytext tinytext collate utf8_bin,
		c_text text collate utf8_bin,
		c_mediumtext mediumtext collate utf8_bin,
		c_longtext longtext collate utf8_bin,
		c_decimal decimal collate utf8_bin,
		c_numeric numeric collate utf8_bin,
		c_enum enum('1') collate utf8_bin,
		c_set set('1') collate utf8_bin,
		c_json json collate utf8_bin)`
	stmts, _, err = parser.Parse(createTableStr, "", "")
	c.Assert(err, IsNil)
}

func (s *testParserSuite) TestAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{"analyze table t1", true, "ANALYZE TABLE `t1`"},
		{"analyze table t,t1", true, "ANALYZE TABLE `t`,`t1`"},
		{"analyze table t1 index", true, "ANALYZE TABLE `t1` INDEX"},
		{"analyze table t1 index a", true, "ANALYZE TABLE `t1` INDEX `a`"},
		{"analyze table t1 index a,b", true, "ANALYZE TABLE `t1` INDEX `a`,`b`"},
		{"analyze table t with 4 buckets", true, "ANALYZE TABLE `t` WITH 4 BUCKETS"},
		{"analyze table t index a with 4 buckets", true, "ANALYZE TABLE `t` INDEX `a` WITH 4 BUCKETS"},
		{"analyze table t partition a", true, "ANALYZE TABLE `t` PARTITION `a`"},
		{"analyze table t partition a with 4 buckets", true, "ANALYZE TABLE `t` PARTITION `a` WITH 4 BUCKETS"},
		{"analyze table t partition a index b", true, "ANALYZE TABLE `t` PARTITION `a` INDEX `b`"},
		{"analyze table t partition a index b with 4 buckets", true, "ANALYZE TABLE `t` PARTITION `a` INDEX `b` WITH 4 BUCKETS"},
		// {"analyze incremental table t index", true, "ANALYZE INCREMENTAL TABLE `t` INDEX"},
		// {"analyze incremental table t index idx", true, "ANALYZE INCREMENTAL TABLE `t` INDEX `idx`"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestGeneratedColumn(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		input string
		ok    bool
		expr  string
	}{
		{"create table t (c int, d int generated always as (c + 1) virtual)", true, "c + 1"},
		{"create table t (c int, d int as (   c + 1   ) virtual)", true, "c + 1"},
		{"create table t (c int, d int as (1 + 1) stored)", true, "1 + 1"},
	}
	parser := New()
	for _, tt := range tests {
		stmtNodes, _, err := parser.Parse(tt.input, "", "")
		if tt.ok {
			c.Assert(err, IsNil)
			stmtNode := stmtNodes[0]
			for _, col := range stmtNode.(*ast.CreateTableStmt).Cols {
				for _, opt := range col.Options {
					if opt.Tp == ast.ColumnOptionGenerated {
						c.Assert(opt.Expr.Text(), Equals, tt.expr)
					}
				}
			}
		} else {
			c.Assert(err, NotNil)
		}
	}

}

func (s *testParserSuite) TestSetTransaction(c *C) {
	defer testleak.AfterTest(c)()
	// Set transaction is equivalent to setting the global or session value of tx_isolation.
	// For example:
	// SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED
	// SET SESSION tx_isolation='READ-COMMITTED'
	tests := []struct {
		input    string
		isGlobal bool
		value    string
	}{
		{
			"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
			false, "READ-COMMITTED",
		},
		{
			"SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			true, "REPEATABLE-READ",
		},
	}
	parser := New()
	for _, t := range tests {
		stmt1, err := parser.ParseOneStmt(t.input, "", "")
		c.Assert(err, IsNil)
		setStmt := stmt1.(*ast.SetStmt)
		vars := setStmt.Variables[0]
		c.Assert(vars.Name, Equals, "tx_isolation")
		c.Assert(vars.IsGlobal, Equals, t.isGlobal)
		c.Assert(vars.IsSystem, Equals, true)
		c.Assert(vars.Value.GetValue(), Equals, t.value)
	}
}

func (s *testParserSuite) TestSideEffect(c *C) {
	// This test cover a bug that parse an error SQL doesn't leave the parser in a
	// clean state, cause the following SQL parse fail.
	parser := New()
	_, err := parser.ParseOneStmt("create table t /*!50100 'abc', 'abc' */;", "", "")
	c.Assert(err, NotNil)

	_, err = parser.ParseOneStmt("show tables;", "", "")
	c.Assert(err, IsNil)
}

func (s *testParserSuite) TestTablePartition(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// {"ALTER TABLE t1 TRUNCATE PARTITION p0", true, "ALTER TABLE `t1` TRUNCATE PARTITION `p0`"},
		// {"ALTER TABLE t1 TRUNCATE PARTITION p0, p1", true, "ALTER TABLE `t1` TRUNCATE PARTITION `p0`,`p1`"},
		{"ALTER TABLE t1 ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT 'APSTART \\' APEND')", true, "ALTER TABLE `t1` ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'APSTART '' APEND')"},
		{"ALTER TABLE t1 ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')", true, "ALTER TABLE `t1` ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')"},
		{`CREATE TABLE t1 (a int not null,b int not null,c int not null,primary key(a,b))
		partition by range (a)
		partitions 3
		(partition x1 values less than (5),
		 partition x2 values less than (10),
		 partition x3 values less than maxvalue);`, true, "CREATE TABLE `t1` (`a` INT NOT NULL,`b` INT NOT NULL,`c` INT NOT NULL,PRIMARY KEY(`a`, `b`)) PARTITION BY RANGE (`a`) (PARTITION `x1` VALUES LESS THAN (5),PARTITION `x2` VALUES LESS THAN (10),PARTITION `x3` VALUES LESS THAN (MAXVALUE))"},
		{"CREATE TABLE t1 (a int not null) partition by range (a) (partition x1 values less than (5) tablespace ts1)", true, "CREATE TABLE `t1` (`a` INT NOT NULL) PARTITION BY RANGE (`a`) (PARTITION `x1` VALUES LESS THAN (5) TABLESPACE = `ts1`)"},
		{`create table t (a int) partition by range (a)
		  (PARTITION p0 VALUES LESS THAN (63340531200) ENGINE = MyISAM,
		   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE MyISAM)`, true, "CREATE TABLE `t` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `p0` VALUES LESS THAN (63340531200) ENGINE = MyISAM,PARTITION `p1` VALUES LESS THAN (63342604800) ENGINE = MyISAM)"},
		{`create table t (a int) partition by range (a)
		  (PARTITION p0 VALUES LESS THAN (63340531200) ENGINE = MyISAM COMMENT 'xxx',
		   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE = MyISAM)`, true, "CREATE TABLE `t` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `p0` VALUES LESS THAN (63340531200) ENGINE = MyISAM COMMENT = 'xxx',PARTITION `p1` VALUES LESS THAN (63342604800) ENGINE = MyISAM)"},
		{`create table t1 (a int) partition by range (a)
		  (PARTITION p0 VALUES LESS THAN (63340531200) COMMENT 'xxx' ENGINE = MyISAM ,
		   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE = MyISAM)`, true, "CREATE TABLE `t1` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `p0` VALUES LESS THAN (63340531200) COMMENT = 'xxx' ENGINE = MyISAM,PARTITION `p1` VALUES LESS THAN (63342604800) ENGINE = MyISAM)"},
		{`create table t (id int)
		    partition by range (id)
		    subpartition by key (id) subpartitions 2
		    (partition p0 values less than (42))`, true, "CREATE TABLE `t` (`id` INT) PARTITION BY RANGE (`id`) SUBPARTITION BY KEY (`id`) SUBPARTITIONS 2 (PARTITION `p0` VALUES LESS THAN (42))"},
		{`create table t (id int)
		    partition by range (id)
		    subpartition by hash (id)
		    (partition p0 values less than (42))`, true, "CREATE TABLE `t` (`id` INT) PARTITION BY RANGE (`id`) SUBPARTITION BY HASH (`id`) (PARTITION `p0` VALUES LESS THAN (42))"},
		{`create table t1 (a varchar(5), b int signed, c varchar(10), d datetime)
		partition by range columns(b,c)
		subpartition by hash(to_seconds(d))
		( partition p0 values less than (2, 'b'),
		  partition p1 values less than (4, 'd'),
		  partition p2 values less than (10, 'za'));`, true,
			"CREATE TABLE `t1` (`a` VARCHAR(5),`b` INT,`c` VARCHAR(10),`d` DATETIME) PARTITION BY RANGE COLUMNS (`b`,`c`) SUBPARTITION BY HASH (TO_SECONDS(`d`)) (PARTITION `p0` VALUES LESS THAN (2, 'b'),PARTITION `p1` VALUES LESS THAN (4, 'd'),PARTITION `p2` VALUES LESS THAN (10, 'za'))"},
		{`CREATE TABLE t1 (a INT, b TIMESTAMP DEFAULT '0000-00-00 00:00:00')
ENGINE=INNODB PARTITION BY LINEAR HASH (a) PARTITIONS 1;`, true, "CREATE TABLE `t1` (`a` INT,`b` TIMESTAMP DEFAULT '0000-00-00 00:00:00') ENGINE = INNODB PARTITION BY LINEAR HASH (`a`) PARTITIONS 1"},

		// empty clause is valid only for HASH/KEY partitions
		{"create table t1 (a int) partition by hash (a) (partition x, partition y)", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`) (PARTITION `x`,PARTITION `y`)"},
		{"create table t1 (a int) partition by key (a) (partition x, partition y)", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY KEY (`a`) (PARTITION `x`,PARTITION `y`)"},
		{"create table t1 (a int) partition by range (a) (partition x, partition y)", false, ""},
		{"create table t1 (a int) partition by list (a) (partition x, partition y)", false, ""},
		{"create table t1 (a int) partition by system_time (partition x, partition y)", false, ""},
		// VALUES LESS THAN clause is valid only for RANGE partitions
		{"create table t1 (a int) partition by hash (a) (partition x values less than (10))", false, ""},
		{"create table t1 (a int) partition by key (a) (partition x values less than (10))", false, ""},
		{"create table t1 (a int) partition by range (a) (partition x values less than (10))", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (10))"},
		{"create table t1 (a int) partition by list (a) (partition x values less than (10))", false, ""},
		{"create table t1 (a int) partition by system_time (partition x values less than (10))", false, ""},
		// VALUES IN clause is valid only for LIST partitions
		{"create table t1 (a int) partition by hash (a) (partition x values in (10))", false, ""},
		{"create table t1 (a int) partition by key (a) (partition x values in (10))", false, ""},
		{"create table t1 (a int) partition by range (a) (partition x values in (10))", false, ""},
		{"create table t1 (a int) partition by list (a) (partition x values in (10))", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY LIST (`a`) (PARTITION `x` VALUES IN (10))"},
		{"create table t1 (a int) partition by system_time (partition x values in (10))", false, ""},
		// HISTORY/CURRENT clauses are valid only for SYSTEM_TIME partitions
		{"create table t1 (a int) partition by hash (a) (partition x history, partition y current)", false, ""},
		{"create table t1 (a int) partition by key (a) (partition x history, partition y current)", false, ""},
		{"create table t1 (a int) partition by range (a) (partition x history, partition y current)", false, ""},
		{"create table t1 (a int) partition by list (a) (partition x history, partition y current)", false, ""},
		{"create table t1 (a int) partition by system_time (partition x history, partition y current)", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)"},

		// LIST, RANGE and SYSTEM_TIME partitions all required definitions
		{"create table t1 (a int) partition by hash (a)", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`) PARTITIONS 1"},
		{"create table t1 (a int) partition by key (a)", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY KEY (`a`) PARTITIONS 1"},
		{"create table t1 (a int) partition by range (a)", false, ""},
		{"create table t1 (a int) partition by list (a)", false, ""},
		{"create table t1 (a int) partition by system_time", false, ""},
		// SYSTEM_TIME required 2 or more partitions
		{"create table t1 (a int) partition by system_time (partition x history)", false, ""},
		{"create table t1 (a int) partition by system_time (partition x current)", false, ""},

		// number of columns and number of values in VALUES clauses must match
		{"create table t1 (a int, b int) partition by range (a) (partition x values less than (10, 20))", false, ""},
		{"create table t (id int) partition by range columns (id) (partition p0 values less than (1, 2))", false, ""},
		{"create table t1 (a int, b int) partition by range columns (a, b) (partition x values less than (10, 20))", true, "CREATE TABLE `t1` (`a` INT,`b` INT) PARTITION BY RANGE COLUMNS (`a`,`b`) (PARTITION `x` VALUES LESS THAN (10, 20))"},
		{"create table t1 (a int, b int) partition by range columns (a, b) (partition x values less than (10))", false, ""},
		{"create table t1 (a int, b int) partition by range columns (a, b) (partition x values less than maxvalue)", false, ""},
		{"create table t1 (a int, b int) partition by list (a) (partition x values in ((10, 20)))", false, ""},
		{"create table t1 (a int, b int) partition by list columns (a, b) (partition x values in ((10, 20)))", true, "CREATE TABLE `t1` (`a` INT,`b` INT) PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))"},
		{"create table t1 (a int, b int) partition by list columns (a, b) (partition x values in (10, 20))", false, ""},
		{"create table t1 (a int, b int) partition by list columns (a, b) (partition x values in (10, (20, 30)))", false, ""},
		{"create table t1 (a int, b int) partition by list columns (a, b) (partition x values in ((10, 20), 30))", false, ""},
		{"create table t1 (a int, b int) partition by list columns (a, b) (partition x values in ((10, 20), (30, 40, 50)))", false, ""},

		// there must be at least one column/partition/value inside (...)
		{"create table t1 (a int) partition by hash (a) ()", false, ""},
		{"create table t1 (a int primary key) partition by key ()", true, "CREATE TABLE `t1` (`a` INT PRIMARY KEY) PARTITION BY KEY () PARTITIONS 1"},
		{"create table t1 (a int) partition by range columns () (partition x values less than maxvalue)", false, ""},
		{"create table t1 (a int) partition by list columns () (partition x default)", false, ""},
		{"create table t1 (a int) partition by range (a) (partition x values less than ())", false, ""},
		{"create table t1 (a int) partition by list (a) (partition x values in ())", false, ""},
		{"create table t1 (a int) partition by list (a) (partition x default)", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)"},

		// only hash and key subpartitions are allowed
		{"create table t1 (a int, b int) partition by range (a) subpartition by range (b) (partition x values less than maxvalue)", false, ""},

		// number of partitions/subpartitions must be matching
		{"create table t1 (a int) partition by hash (a) partitions 2 (partition x)", false, ""},
		{"create table t1 (a int) partition by hash (a) partitions 2 (partition x, partition y)", true, "CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`) (PARTITION `x`,PARTITION `y`)"},
		{"create table t1 (a int, b int) partition by range (a) subpartition by hash (b) subpartitions 2 (partition x values less than maxvalue (subpartition y))", false, ""},
		{
			"create table t1 (a int, b int) partition by range (a) subpartition by hash (b) subpartitions 2 (partition x values less than maxvalue (subpartition y, subpartition z))", true,
			"CREATE TABLE `t1` (`a` INT,`b` INT) PARTITION BY RANGE (`a`) SUBPARTITION BY HASH (`b`) SUBPARTITIONS 2 (PARTITION `x` VALUES LESS THAN (MAXVALUE) (SUBPARTITION `y`,SUBPARTITION `z`))",
		},
		{
			"create table t1 (a int, b int) partition by range (a) subpartition by hash (b) (partition x values less than (10) (subpartition y,subpartition z),partition a values less than (20) (subpartition b,subpartition c))", true,
			"CREATE TABLE `t1` (`a` INT,`b` INT) PARTITION BY RANGE (`a`) SUBPARTITION BY HASH (`b`) SUBPARTITIONS 2 (PARTITION `x` VALUES LESS THAN (10) (SUBPARTITION `y`,SUBPARTITION `z`),PARTITION `a` VALUES LESS THAN (20) (SUBPARTITION `b`,SUBPARTITION `c`))",
		},
		{"create table t1 (a int, b int) partition by range (a) subpartition by hash (b) (partition x values less than (10) (subpartition y),partition a values less than (20) (subpartition b,subpartition c))", false, ""},
		{"create table t1 (a int, b int) partition by range (a) (partition x values less than (10) (subpartition y))", false, ""},
		{"create table t1 (a int) partition by hash (a) partitions 0", false, ""},
		{"create table t1 (a int, b int) partition by range (a) subpartition by hash (b) subpartitions 0 (partition x values less than (10))", false, ""},

		// other partition tests
		{"create table t1 (a int) partition by system_time interval 7 day limit 50000 (partition x history, partition y current)", false, ""},
		{
			"create table t1 (a int) partition by system_time interval 7 day (partition x history, partition y current)", true,
			"CREATE TABLE `t1` (`a` INT) PARTITION BY SYSTEM_TIME INTERVAL 7 DAY (PARTITION `x` HISTORY,PARTITION `y` CURRENT)",
		},
		{
			"create table t1 (a int) partition by system_time limit 50000 (partition x history, partition y current)", true,
			"CREATE TABLE `t1` (`a` INT) PARTITION BY SYSTEM_TIME LIMIT 50000 (PARTITION `x` HISTORY,PARTITION `y` CURRENT)",
		},
		{
			"create table t1 (a int) partition by hash(a) (partition x engine InnoDB comment 'xxxx' data directory '/var/data' index directory '/var/index' max_rows 70000 min_rows 50 tablespace `innodb_file_per_table` nodegroup 255)", true,
			"CREATE TABLE `t1` (`a` INT) PARTITION BY HASH (`a`) (PARTITION `x` ENGINE = InnoDB COMMENT = 'xxxx' DATA DIRECTORY = '/var/data' INDEX DIRECTORY = '/var/index' MAX_ROWS = 70000 MIN_ROWS = 50 TABLESPACE = `innodb_file_per_table` NODEGROUP = 255)",
		},
		{
			"create table t1 (a int, b int) partition by range(a) subpartition by hash(b) (partition x values less than maxvalue (subpartition y engine InnoDB comment 'xxxx' data directory '/var/data' index directory '/var/index' max_rows 70000 min_rows 50 tablespace `innodb_file_per_table` nodegroup 255))", true,
			"CREATE TABLE `t1` (`a` INT,`b` INT) PARTITION BY RANGE (`a`) SUBPARTITION BY HASH (`b`) SUBPARTITIONS 1 (PARTITION `x` VALUES LESS THAN (MAXVALUE) (SUBPARTITION `y` ENGINE = InnoDB COMMENT = 'xxxx' DATA DIRECTORY = '/var/data' INDEX DIRECTORY = '/var/index' MAX_ROWS = 70000 MIN_ROWS = 50 TABLESPACE = `innodb_file_per_table` NODEGROUP = 255))",
		},
	}
	s.RunTest(c, table, false)

	// Check comment content.
	parser := New()
	stmt, err := parser.ParseOneStmt("create table t (id int) partition by range (id) (partition p0 values less than (10) comment 'check')", "", "")
	c.Assert(err, IsNil)
	createTable := stmt.(*ast.CreateTableStmt)
	comment, ok := createTable.Partition.Definitions[0].Comment()
	c.Assert(ok, IsTrue)
	c.Assert(comment, Equals, "check")
}

func (s *testParserSuite) TestNotExistsSubquery(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{`select * from t1 where not exists (select * from t2 where t1.a = t2.a)`, true, ""},
	}

	parser := New()
	for _, tt := range table {
		stmt, _, err := parser.Parse(tt.src, "", "")
		c.Assert(err, IsNil)

		sel := stmt[0].(*ast.SelectStmt)
		exists, ok := sel.Where.(*ast.ExistsSubqueryExpr)
		c.Assert(ok, IsTrue)
		c.Assert(exists.Not, Equals, tt.ok)
	}
}

func (s *testParserSuite) TestFulltextSearch(c *C) {
	parser := New()

	st, err := parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search')", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.SelectStmt), NotNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH() AGAINST('search')", "", "")
	c.Assert(err, NotNil)
	c.Assert(st, IsNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST()", "", "")
	c.Assert(err, NotNil)
	c.Assert(st, IsNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search' IN)", "", "")
	c.Assert(err, NotNil)
	c.Assert(st, IsNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(content) AGAINST('search' IN BOOLEAN MODE WITH QUERY EXPANSION)", "", "")
	c.Assert(err, NotNil)
	c.Assert(st, IsNil)

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' IN NATURAL LANGUAGE MODE)", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.SelectStmt), NotNil)
	writer := bytes.NewBufferString("")
	st.(*ast.SelectStmt).Where.Format(writer)
	c.Assert(writer.String(), Equals, "MATCH(title,content) AGAINST(\"search\")")

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' IN BOOLEAN MODE)", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.SelectStmt), NotNil)
	writer.Reset()
	st.(*ast.SelectStmt).Where.Format(writer)
	c.Assert(writer.String(), Equals, "MATCH(title,content) AGAINST(\"search\" IN BOOLEAN MODE)")

	st, err = parser.ParseOneStmt("SELECT * FROM fulltext_test WHERE MATCH(title,content) AGAINST('search' WITH QUERY EXPANSION)", "", "")
	c.Assert(err, IsNil)
	c.Assert(st.(*ast.SelectStmt), NotNil)
	writer.Reset()
	st.(*ast.SelectStmt).Where.Format(writer)
	c.Assert(writer.String(), Equals, "MATCH(title,content) AGAINST(\"search\" WITH QUERY EXPANSION)")
}

func (s *testParserSuite) TestTableSample(c *C) {
	table := []testCase{
		// positive test cases
		{"select * from tbl tablesample system (50);", true, "SELECT * FROM `tbl` TABLESAMPLE SYSTEM (50)"},
		{"select * from tbl tablesample system (50 percent);", true, "SELECT * FROM `tbl` TABLESAMPLE SYSTEM (50 PERCENT)"},
		{"select * from tbl tablesample system (49.9 percent);", true, "SELECT * FROM `tbl` TABLESAMPLE SYSTEM (49.9 PERCENT)"},
		{"select * from tbl tablesample system (120 rows);", true, "SELECT * FROM `tbl` TABLESAMPLE SYSTEM (120 ROWS)"},
		{"select * from tbl tablesample bernoulli (50);", true, "SELECT * FROM `tbl` TABLESAMPLE BERNOULLI (50)"},
		{"select * from tbl tablesample (50);", true, "SELECT * FROM `tbl` TABLESAMPLE (50)"},
		{"select * from tbl tablesample (50) repeatable (123456789);", true, "SELECT * FROM `tbl` TABLESAMPLE (50) REPEATABLE(123456789)"},
		{"select * from tbl as a tablesample (50);", true, "SELECT * FROM `tbl` AS `a` TABLESAMPLE (50)"},
		{"select * from tbl `tablesample` tablesample (50);", true, "SELECT * FROM `tbl` AS `tablesample` TABLESAMPLE (50)"},
		{"select * from tbl tablesample (50) where id > 20;", true, "SELECT * FROM `tbl` TABLESAMPLE (50) WHERE `id`>20"},
		{"select * from tbl partition (p0) tablesample (50);", true, "SELECT * FROM `tbl` PARTITION(`p0`) TABLESAMPLE (50)"},
		{"select * from tbl tablesample (0 percent);", true, "SELECT * FROM `tbl` TABLESAMPLE (0 PERCENT)"},
		{"select * from tbl tablesample (100 percent);", true, "SELECT * FROM `tbl` TABLESAMPLE (100 PERCENT)"},
		{"select * from tbl tablesample (0 rows);", true, "SELECT * FROM `tbl` TABLESAMPLE (0 ROWS)"},
		{"select * from tbl tablesample ('34');", true, "SELECT * FROM `tbl` TABLESAMPLE ('34')"},
		{"select * from tbl1 tablesample (10), tbl2 tablesample (20);", true, "SELECT * FROM (`tbl1` TABLESAMPLE (10)) JOIN `tbl2` TABLESAMPLE (20)"},
		{"select * from tbl1 a tablesample (10) join tbl2 b tablesample (20) on a.id <> b.id;", true, "SELECT * FROM `tbl1` AS `a` TABLESAMPLE (10) JOIN `tbl2` AS `b` TABLESAMPLE (20) ON `a`.`id`!=`b`.`id`"},

		// negative test cases
		{"select * from tbl tablesample system(50) a;", false, ""},
		{"select * from tbl tablesample (50) partition (p0);", false, ""},
		{"select * from tbl where id > 20 tablesample system(50);", false, ""},
		{"select * from (select * from tbl) a tablesample system(50);", false, ""},
		{"select * from tbl tablesample system(50) tablesample system(50);", false, ""},
		{"select * from tbl tablesample system(50, 50);", false, ""},
		{"select * from tbl tablesample dhfksdlfljcoew(50);", false, ""},
		{"select * from tbl tablesample system;", false, ""},
		{"select * from tbl tablesample system (33) repeatable;", false, ""},
		{"select 1 from dual tablesample system (50);", false, ""},
	}
	s.RunTest(c, table, false)
	p := New()
	cases := []string{
		"select * from tbl tablesample (33.3 + 44.4);",
		"select * from tbl tablesample (33.3 + 44.4 percent);",
		"select * from tbl tablesample (33 + 44 rows);",
		"select * from tbl tablesample (33 + 44 rows) repeatable (55 + 66);",
		"select * from tbl tablesample (200);",
		"select * from tbl tablesample (-10);",
		"select * from tbl tablesample (null);",
		"select * from tbl tablesample (33.3 rows);",
		"select * from tbl tablesample (-4 rows);",
		"select * from tbl tablesample (50) repeatable ('ssss');",
		"delete from tbl using tbl2 tablesample(10 rows) repeatable (111) where tbl.id = tbl2.id",
		"update tbl tablesample regions() set id = '1'",
	}
	for _, sql := range cases {
		comment := Commentf("source %v", sql)
		_, err := p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
	}
}

// For CTE
func (s *testParserSuite) TestCTE(c *C) {
	table := []testCase{
		{"WITH `cte` AS (SELECT 1,2) SELECT `col1`,`col2` FROM `cte`", true, "WITH `cte` AS (SELECT 1,2) SELECT `col1`,`col2` FROM `cte`"},
		{"WITH `cte` (col1, col2) AS (SELECT 1,2 UNION ALL SELECT 3,4) SELECT col1, col2 FROM cte;", true, "WITH `cte` (`col1`, `col2`) AS (SELECT 1,2 UNION ALL SELECT 3,4) SELECT `col1`,`col2` FROM `cte`"},
		{"WITH `cte` AS (SELECT 1,2), cte2 as (select 3) SELECT `col1`,`col2` FROM `cte`", true, "WITH `cte` AS (SELECT 1,2), `cte2` AS (SELECT 3) SELECT `col1`,`col2` FROM `cte`"},
		{"WITH RECURSIVE cte (n) AS (  SELECT 1  UNION ALL  SELECT n + 1 FROM cte WHERE n < 5)SELECT * FROM cte;", true, "WITH RECURSIVE `cte` (`n`) AS (SELECT 1 UNION ALL SELECT `n`+1 FROM `cte` WHERE `n`<5) SELECT * FROM `cte`"},
		{"with cte(a) as (select 1) update t, cte set t.a=1  where t.a=cte.a;", true, "WITH `cte` (`a`) AS (SELECT 1) UPDATE (`t`) JOIN `cte` SET `t`.`a`=1 WHERE `t`.`a`=`cte`.`a`"},
		{"with cte(a) as (select 1) delete t from t, cte where t.a=cte.a;", true, "WITH `cte` (`a`) AS (SELECT 1) DELETE `t` FROM (`t`) JOIN `cte` WHERE `t`.`a`=`cte`.`a`"},
		{"WITH cte1 AS (SELECT 1) SELECT * FROM (WITH cte2 AS (SELECT 2) SELECT * FROM cte2 JOIN cte1) AS dt;", true, "WITH `cte1` AS (SELECT 1) SELECT * FROM (WITH `cte2` AS (SELECT 2) SELECT * FROM `cte2` JOIN `cte1`) AS `dt`"},
		{"WITH cte AS (SELECT 1) SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM cte;", true, "WITH `cte` AS (SELECT 1) SELECT /*+ MAX_EXECUTION_TIME(1000)*/ * FROM `cte`"},
		{"with cte as (table t) table cte;", true, "WITH `cte` AS (TABLE `t`) TABLE `cte`"},
		{"with cte as (select 1) select 1 union with cte as (select 1) select * from cte;", false, ""},
		{"with cte as (select 1) (select 1);", true, "WITH `cte` AS (SELECT 1) (SELECT 1)"},
		{"with cte as (select 1) (select 1 union select 1)", true, "WITH `cte` AS (SELECT 1) (SELECT 1 UNION SELECT 1)"},
		{"select * from (with cte as (select 1) select 1 union select 2) qn", true, "SELECT * FROM (WITH `cte` AS (SELECT 1) SELECT 1 UNION SELECT 2) AS `qn`"},
		{"select * from t where 1 > (with cte as (select 2) select * from cte)", true, "SELECT * FROM `t` WHERE 1>(WITH `cte` AS (SELECT 2) SELECT * FROM `cte`)"},
		{"( with cte(n) as ( select 1 )  select n+1 from cte  union select n+2 from cte) union select 1", true, "(WITH `cte` (`n`) AS (SELECT 1) SELECT `n`+1 FROM `cte` UNION SELECT `n`+2 FROM `cte`) UNION SELECT 1"},
		{"( with cte(n) as ( select 1 )  select n+1 from cte) union select 1", true, "(WITH `cte` (`n`) AS (SELECT 1) SELECT `n`+1 FROM `cte`) UNION SELECT 1"},
		{"( with cte(n) as ( select 1 )  (select n+1 from cte)) union select 1", true, "(WITH `cte` (`n`) AS (SELECT 1) (SELECT `n`+1 FROM `cte`)) UNION SELECT 1"},
	}

	s.RunTest(c, table, false)
}

// For CTE Merge
func (s *testParserSuite) TestCTEMerge(c *C) {
	table := []testCase{
		{"WITH `cte` AS (SELECT 1,2) SELECT `col1`,`col2` FROM `cte`", true, "WITH `cte` AS (SELECT 1,2) SELECT `col1`,`col2` FROM `cte`"},
		{"WITH `cte` (col1, col2) AS (SELECT 1,2 UNION ALL SELECT 3,4) SELECT col1, col2 FROM cte;", true, "WITH `cte` (`col1`, `col2`) AS (SELECT 1,2 UNION ALL SELECT 3,4) SELECT `col1`,`col2` FROM `cte`"},
		{"WITH `cte` AS (SELECT 1,2), cte2 as (select 3) SELECT `col1`,`col2` FROM `cte`", true, "WITH `cte` AS (SELECT 1,2), `cte2` AS (SELECT 3) SELECT `col1`,`col2` FROM `cte`"},
		{"with cte(a) as (select 1) update t, cte set t.a=1  where t.a=cte.a;", true, "WITH `cte` (`a`) AS (SELECT 1) UPDATE (`t`) JOIN `cte` SET `t`.`a`=1 WHERE `t`.`a`=`cte`.`a`"},
		{"with cte(a) as (select 1) delete t from t, cte where t.a=cte.a;", true, "WITH `cte` (`a`) AS (SELECT 1) DELETE `t` FROM (`t`) JOIN `cte` WHERE `t`.`a`=`cte`.`a`"},
		{"WITH cte1 AS (SELECT 1) SELECT * FROM (WITH cte2 AS (SELECT 2) SELECT * FROM cte2 JOIN cte1) AS dt;", true, "WITH `cte1` AS (SELECT 1) SELECT * FROM (WITH `cte2` AS (SELECT 2) SELECT * FROM `cte2` JOIN `cte1`) AS `dt`"},
		{"WITH cte AS (SELECT 1) SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM cte;", true, "WITH `cte` AS (SELECT 1) SELECT /*+ MAX_EXECUTION_TIME(1000)*/ * FROM `cte`"},
		{"with cte as (table t) table cte;", true, "WITH `cte` AS (TABLE `t`) TABLE `cte`"},
		{"with cte as (select 1) select 1 union with cte as (select 1) select * from cte;", false, ""},
		{"with cte as (select 1) (select 1);", true, "WITH `cte` AS (SELECT 1) (SELECT 1)"},
		{"with cte as (select 1) (select 1 union select 1)", true, "WITH `cte` AS (SELECT 1) (SELECT 1 UNION SELECT 1)"},
		{"select * from (with cte as (select 1) select 1 union select 2) qn", true, "SELECT * FROM (WITH `cte` AS (SELECT 1) SELECT 1 UNION SELECT 2) AS `qn`"},
		{"select * from t where 1 > (with cte as (select 2) select * from cte)", true, "SELECT * FROM `t` WHERE 1>(WITH `cte` AS (SELECT 2) SELECT * FROM `cte`)"},
		{"( with cte(n) as ( select 1 )  select n+1 from cte  union select n+2 from cte) union select 1", true, "(WITH `cte` (`n`) AS (SELECT 1) SELECT `n`+1 FROM `cte` UNION SELECT `n`+2 FROM `cte`) UNION SELECT 1"},
		{"( with cte(n) as ( select 1 )  select n+1 from cte) union select 1", true, "(WITH `cte` (`n`) AS (SELECT 1) SELECT `n`+1 FROM `cte`) UNION SELECT 1"},
		{"( with cte(n) as ( select 1 )  (select n+1 from cte)) union select 1", true, "(WITH `cte` (`n`) AS (SELECT 1) (SELECT `n`+1 FROM `cte`)) UNION SELECT 1"},
	}

	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestIntervalPartition(c *C) {
	table := []testCase{
		{"CREATE TABLE t (c1 integer,c2 integer) PARTITION BY RANGE (c1) INTERVAL (1000)", true, "CREATE TABLE `t` (`c1` INT,`c2` INT) PARTITION BY RANGE (`c1`) INTERVAL (1000)"},
		{"CREATE TABLE t (c1 int, c2 date) PARTITION BY RANGE (c2) INTERVAL (1 Month)", true, "CREATE TABLE `t` (`c1` INT,`c2` DATE) PARTITION BY RANGE (`c2`) INTERVAL (1 MONTH)"},
		{"CREATE TABLE t (c1 int, c2 date) PARTITION BY RANGE (c1) (partition p1 values less than (22))", true, "CREATE TABLE `t` (`c1` INT,`c2` DATE) PARTITION BY RANGE (`c1`) (PARTITION `p1` VALUES LESS THAN (22))"},
		{`CREATE TABLE t (c1 int, c2 date) PARTITION BY RANGE COLUMNS (c2) INTERVAL (1 year) first partition less than ("2022-02-01")`, false, ""},
		{`CREATE TABLE t (c1 int, c2 datetime) PARTITION BY RANGE COLUMNS (c2) INTERVAL (1 day) first partition less than ("2022-01-02") last partition less than ("2022-06-01") NULL PARTITION MAXVALUE PARTITION`, true, "CREATE TABLE `t` (`c1` INT,`c2` DATETIME) PARTITION BY RANGE COLUMNS (`c2`) INTERVAL (1 DAY) FIRST PARTITION LESS THAN (_UTF8MB4'2022-01-02') LAST PARTITION LESS THAN (_UTF8MB4'2022-06-01') NULL PARTITION MAXVALUE PARTITION"},
		{`ALTER TABLE t LAST PARTITION LESS THAN (1000)`, true, "ALTER TABLE `t` LAST PARTITION LESS THAN (1000)"},
		{`ALTER TABLE t REORGANIZE MAX PARTITION INTO NEW LAST PARTITION LESS THAN (1000)`, false, ""},
		{`ALTER TABLE t REORGANIZE MAX PARTITION INTO LAST PARTITION LESS THAN (1000)`, false, ""},
		{`ALTER TABLE t REORGANIZE MAXVALUE PARTITION INTO NEW LAST PARTITION LESS THAN (1000)`, false, ""},
		{`ALTER TABLE t REORGANIZE MAXVALUE PARTITION INTO LAST PARTITION LESS THAN (1000)`, false, ""},
		{`ALTER TABLE t split MAXVALUE PARTITION LESS THAN (1000)`, true, "ALTER TABLE `t` SPLIT MAXVALUE PARTITION LESS THAN (1000)"},
		{`ALTER TABLE t merge first PARTITION LESS THAN (1000)`, true, "ALTER TABLE `t` MERGE FIRST PARTITION LESS THAN (1000)"},
		{`ALTER TABLE t first PARTITION LESS THAN (1000)`, true, "ALTER TABLE `t` FIRST PARTITION LESS THAN (1000)"},
	}

	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestAsOfClause(c *C) {
	table := []testCase{
		{"SELECT * FROM `t` AS /* comment */ a;", true, "SELECT * FROM `t` AS `a`"},
		{"SELECT * FROM `t` AS OF TIMESTAMP READ_TS_IN(DATE_SUB(NOW(), INTERVAL 3 SECOND), NOW());", true, "SELECT * FROM `t` AS OF TIMESTAMP READ_TS_IN(DATE_SUB(NOW(), INTERVAL 3 SECOND), NOW())"},
		{"select * from `t` as of timestamp '2021-04-15 00:00:00'", true, "SELECT * FROM `t` AS OF TIMESTAMP _UTF8MB4'2021-04-15 00:00:00'"},
		{"SELECT * FROM (`a` AS OF TIMESTAMP READ_TS_IN(DATE_SUB(NOW(), INTERVAL 3 SECOND), NOW())) JOIN `b` AS OF TIMESTAMP READ_TS_IN(DATE_SUB(NOW(), INTERVAL 3 SECOND), NOW());", true, "SELECT * FROM (`a` AS OF TIMESTAMP READ_TS_IN(DATE_SUB(NOW(), INTERVAL 3 SECOND), NOW())) JOIN `b` AS OF TIMESTAMP READ_TS_IN(DATE_SUB(NOW(), INTERVAL 3 SECOND), NOW())"},
		{"INSERT INTO `employees` (SELECT * FROM `employees` AS OF TIMESTAMP (DATE_SUB(NOW(), INTERVAL _UTF8MB4'60' MINUTE)) NOT IN (SELECT * FROM `employees`))", true, "INSERT INTO `employees` (SELECT * FROM `employees` AS OF TIMESTAMP (DATE_SUB(NOW(), INTERVAL _UTF8MB4'60' MINUTE)) NOT IN (SELECT * FROM `employees`))"},
		{"SET TRANSACTION READ ONLY as of timestamp '2021-04-21 00:42:12'", true, "SET @@SESSION.`tx_read_only`=_UTF8MB4'1', @@SESSION.`tx_read_ts`=_UTF8MB4'2021-04-21 00:42:12'"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestWithRollup(c *C) {
	table := []testCase{
		{`select * from t group by a, b rollup`, false, ""},
		{`select * from t group by a, b with rollup`, true, "SELECT * FROM `t` GROUP BY `a`,`b` WITH ROLLUP"},
		// should be ERROR 1241 (21000): Operand should contain 1 column(s) in runtime.
		{`select * from t group by (a, b) with rollup`, true, "SELECT * FROM `t` GROUP BY ROW(`a`,`b`) WITH ROLLUP"},
		{`select * from t group by (a+b) with rollup`, true, "SELECT * FROM `t` GROUP BY (`a`+`b`) WITH ROLLUP"},
	}
	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestVector(c *C) {
	table := []testCase{
		{"CREATE TABLE t (a VECTOR)", true, "CREATE TABLE `t` (`a` VECTOR)"},
		{"CREATE TABLE t (a VECTOR<FLOAT>)", true, "CREATE TABLE `t` (`a` VECTOR)"},
		{"CREATE TABLE t (a VECTOR(3))", true, "CREATE TABLE `t` (`a` VECTOR(3))"},
		{"CREATE TABLE t (a VECTOR<FLOAT>(3))", true, "CREATE TABLE `t` (`a` VECTOR(3))"},
		{"CREATE TABLE t (a VECTOR<INT>)", false, ""},
		{"CREATE TABLE t (a VECTOR<DOUBLE>)", false, ""},
		{"CREATE TABLE t (a VECTOR<ABC>)", false, ""},
		{"CREATE TABLE t (a VECTOR(5)<FLOAT>)", false, ""},
	}

	s.RunTest(c, table, false)
}

func (s *testParserSuite) TestSpatial(c *C) {
	table := []testCase{
		{"CREATE TABLE t (a GEOMETRY)", true, "CREATE TABLE `t` (`a` GEOMETRY)"},
		{"CREATE TABLE t (a GEOMETRY NOT NULL SRID 4326)", true, "CREATE TABLE `t` (`a` GEOMETRY NOT NULL SRID 4326)"},
		{"CREATE TABLE t (a POINT)", true, "CREATE TABLE `t` (`a` POINT)"},
		{"CREATE TABLE t (a POINT SRID 0)", true, "CREATE TABLE `t` (`a` POINT SRID 0)"},
		{"CREATE TABLE t (a LINESTRING)", true, "CREATE TABLE `t` (`a` LINESTRING)"},
		{"CREATE TABLE t (a POLYGON)", true, "CREATE TABLE `t` (`a` POLYGON)"},
		{"CREATE TABLE t (a MULTIPOLYGON)", true, "CREATE TABLE `t` (`a` MULTIPOLYGON)"},
		{"CREATE TABLE t (a MULTIPOINT)", true, "CREATE TABLE `t` (`a` MULTIPOINT)"},
		{"CREATE TABLE t (a MULTILINESTRING)", true, "CREATE TABLE `t` (`a` MULTILINESTRING)"},
		{"CREATE TABLE t (a GEOMETRYCOLLECTION)", true, "CREATE TABLE `t` (`a` GEOMETRYCOLLECTION)"},
	}

	s.RunTest(c, table, false)
}

// nodeTextCleaner clean the text of a node and it's child node.
// For test only.
type nodeTextCleaner struct {
}

// Enter implements Visitor interface.
// Enter implements Visitor interface.
func (checker *nodeTextCleaner) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	in.SetText("")
	switch node := in.(type) {
	case *ast.Constraint:
		if node.Option != nil {
			if node.Option.KeyBlockSize == 0x0 && node.Option.Tp == 0 && node.Option.Comment == "" {
				node.Option = nil
			}
		}
	case *ast.FuncCallExpr:
		node.FnName.O = strings.ToLower(node.FnName.O)
		switch node.FnName.L {
		case "convert":
			node.Args[1].(*ast.ValueExpr).Datum.SetBytes(nil)
		}
	case *ast.AggregateFuncExpr:
		node.F = strings.ToLower(node.F)
	case *ast.FieldList:
		for _, f := range node.Fields {
			f.Offset = 0
		}
	case *ast.AlterTableSpec:
		for _, opt := range node.Options {
			opt.StrValue = strings.ToLower(opt.StrValue)
		}
	case *ast.Join:
		node.ExplicitParens = false
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *nodeTextCleaner) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
