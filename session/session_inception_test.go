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

package session_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hanchuanchuan/goInception/config"
	"github.com/hanchuanchuan/goInception/session"
	"github.com/hanchuanchuan/goInception/util/testkit"
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

var _ = Suite(&testSessionIncSuite{})

type SQLError = session.SQLError

// SQLAudit 审核类
type SQLAudit struct {
	sql    string
	errors []*SQLError
}

func TestAudit(t *testing.T) {
	TestingT(t)
}

type testSessionIncSuite struct {
	testCommon

	audits []SQLAudit
}

func (s *testSessionIncSuite) add(sql string, errors ...*SQLError) {
	s.audits = append(s.audits, SQLAudit{
		sql:    sql,
		errors: errors,
	})
}

func (s *testSessionIncSuite) SetUpSuite(c *C) {

	s.initSetUp(c)

	inc := &s.defaultInc
	inc.EnableFingerprint = true
	inc.SqlSafeUpdates = 0
	inc.EnableDropTable = true
	inc.EnableIdentiferKeyword = true
}

func (s *testSessionIncSuite) TearDownSuite(c *C) {
	s.tearDownSuite(c)
}

func (s *testSessionIncSuite) TearDownTest(c *C) {
	s.reset()
	s.tearDownTest(c)
}

func (s *testSessionIncSuite) testErrorCode(c *C, sql string, errors ...*session.SQLError) {

	if s.isAPI {
		s.sessionService.LoadOptions(session.SourceOptions{
			Host:         s.defaultInc.BackupHost,
			Port:         int(s.defaultInc.BackupPort),
			User:         s.defaultInc.BackupUser,
			Password:     s.defaultInc.BackupPassword,
			RealRowCount: s.realRowCount,
		})
		s.testAuditResult(c, sql, errors...)
		return
	}

	if s.tk == nil {
		s.tk = testkit.NewTestKitWithInit(c, s.store)
	}

	// session.CheckAuditSetting(config.GetGlobalConfig())

	s.runCheck(sql)
	row := s.rows[s.getAffectedRows()-1]

	errCode := 0
	if len(errors) > 0 {
		for _, e := range errors {
			level := session.GetErrorLevel(e.Code)
			if int(level) > errCode {
				errCode = int(level)
			}
		}
	}

	if errCode > 0 {
		errMsgs := []string{}
		for _, e := range errors {
			errMsgs = append(errMsgs, e.Error())
		}
		c.Assert(row[4], Equals, strings.Join(errMsgs, "\n"), Commentf("%v", s.rows))
	}

	c.Assert(row[2], Equals, strconv.Itoa(errCode), Commentf("%v", row))
}

func (s *testSessionIncSuite) testAuditResult(c *C, sql string, errors ...*session.SQLError) {

	result, err := s.sessionService.Audit(context.Background(), s.useDB+sql)
	c.Assert(err, IsNil)
	// for _, row := range result {
	// 	if row.ErrLevel == 2 {
	// 		fmt.Println(fmt.Sprintf("sql: %v, err: %v", row.Sql, row.ErrorMessage))
	// 	} else {
	// 		fmt.Println(fmt.Sprintf("[%v] sql: %v", session.StatusList[row.StageStatus], row.Sql))
	// 	}
	// }

	s.records = result

	row := result[len(result)-1]

	errCode := uint8(0)
	if len(errors) > 0 {
		for _, e := range errors {
			level := session.GetErrorLevel(e.Code)
			if level > errCode {
				errCode = level
			}
		}
	}

	if errCode > 0 {
		errMsgs := []string{}
		for _, e := range errors {
			errMsgs = append(errMsgs, e.Error())
		}
		c.Assert(row.ErrorMessage, Equals, strings.Join(errMsgs, "\n"), Commentf("%v", result))
	}

	c.Assert(row.ErrLevel, Equals, errCode, Commentf("%#v", row))
}

func (s *testSessionIncSuite) testManyErrors(c *C, sql string, errors ...*session.SQLError) {
	if s.tk == nil {
		s.tk = testkit.NewTestKitWithInit(c, s.store)
	}

	s.runCheck(sql)

	errCode := 0
	if len(errors) > 0 {
		for _, e := range errors {
			level := session.GetErrorLevel(e.Code)
			if int(level) > errCode {
				errCode = int(level)
			}
		}
	}

	allErrors := []string{}
	for _, row := range s.getResultRows() {
		if v, ok := row[4].(string); ok {
			v = strings.TrimSpace(v)
			if v != "<nil>" && v != "" {
				allErrors = append(allErrors, v)
			}
		}
	}

	errMsgs := []string{}
	for _, e := range errors {
		errMsgs = append(errMsgs, e.Error())
	}

	// c.Assert(len(errMsgs), Equals, len(allErrors), Commentf("%v", allErrors))
	c.Assert(len(errMsgs), Equals, len(allErrors), Commentf("%#v", s.rows))

	for index, err := range allErrors {
		c.Assert(err, Equals, errMsgs[index], Commentf("%v", s.rows))
	}

}

func (s *testSessionIncSuite) runAudit(c *C) {

	c.Assert(len(s.audits), Not(Equals), 0)

	var sqls []string
	for _, a := range s.audits {
		sqls = append(sqls, a.sql)
	}

	a := `/*%s;--check=1;--backup=0;--enable-ignore-warnings;real_row_count=%v;*/
inception_magic_start;
%s
%s;
inception_magic_commit;`
	res := s.tk.MustQueryInc(fmt.Sprintf(a, s.getAddr(), s.realRowCount, s.useDB,
		strings.Join(sqls, ";\n"),
	))
	s.rows = res.Rows()
	rows := s.rows

	c.Assert(len(s.audits), Equals, len(rows)-1)

	for index, row := range rows {
		errors := s.audits[index].errors
		errCode := 0
		if len(errors) > 0 {
			for _, e := range errors {
				level := session.GetErrorLevel(e.Code)
				if int(level) > errCode {
					errCode = int(level)
				}
			}
		}

		if errCode > 0 {
			errMsgs := []string{}
			for _, e := range errors {
				errMsgs = append(errMsgs, e.Error())
			}
			c.Assert(row[4], Equals, strings.Join(errMsgs, "\n"), Commentf("%v", rows))
		}
		c.Assert(row[2], Equals, strconv.Itoa(errCode), Commentf("%v", row))
	}

	s.audits = nil
}

func (s *testCommon) assertAudit(c *C,
	rows [][]interface{},
	allErrors ...[]*SQLError) {

	c.Assert(len(rows), Not(Equals), 0)
	c.Assert(len(rows), Equals, len(allErrors), Commentf("%v", rows))

	for index, row := range rows {
		errors := allErrors[index]
		errCode := 0
		if len(errors) > 0 {
			for _, e := range errors {
				level := session.GetErrorLevel(e.Code)
				if int(level) > errCode {
					errCode = int(level)
				}
			}
		}

		if errCode > 0 {
			errMsgs := []string{}
			for _, e := range errors {
				errMsgs = append(errMsgs, e.Error())
			}
			c.Assert(row[4], Equals, strings.Join(errMsgs, "\n"), Commentf("%v", rows))
		}
		if s.isAPI {
			c.Assert(row[2], Equals, int64(errCode), Commentf("%v", row))
		} else {
			c.Assert(row[2], Equals, strconv.Itoa(errCode), Commentf("%v", row))
		}
	}
}

func (s *testSessionIncSuite) TestBegin(c *C) {
	if testing.Short() {
		c.Skip("skipping test; in TRAVIS mode")
	}

	res := s.tk.MustQueryInc("create table t1(id int);")
	s.rows = res.Rows()
	c.Assert(len(s.rows), Equals, 1, Commentf("%v", s.rows))

	for _, row := range s.rows {
		c.Assert(row[2], Equals, "2")
		c.Assert(row[4], Equals, "Must start as begin statement.")
	}
}

func (s *testSessionIncSuite) TestNoSourceInfo(c *C) {
	res := s.tk.MustQueryInc(`inception_magic_start;
	create table t1(id int);`)
	s.rows = res.Rows()
	c.Assert(s.getAffectedRows(), Equals, 1)

	for _, row := range s.rows {
		c.Assert(row[2], Equals, "2")
		c.Assert(row[4], Equals, "Invalid source infomation(inception语法格式错误).")
	}
}

func (s *testSessionIncSuite) TestNoSourceInfo2(c *C) {
	res := s.tk.MustQueryInc(`/*--check=1;*/
	inception_magic_start;create table t1(id int)`)
	s.rows = res.Rows()
	c.Assert(s.getAffectedRows(), Equals, 1)

	for _, row := range s.rows {
		c.Assert(row[2], Equals, "2")
		c.Assert(row[4], Equals, "Invalid source infomation(主机名为空,端口为0,用户名为空).")
	}
}

func (s *testSessionIncSuite) TestWrongDBName(c *C) {
	res := s.tk.MustQueryInc(fmt.Sprintf(`/*%s;--check=1;--backup=0;--ignore-warnings=1;*/
	inception_magic_start;create table t1(id int);inception_magic_commit;`, s.getAddr()))
	s.rows = res.Rows()
	c.Assert(s.getAffectedRows(), Equals, 1)

	for _, row := range s.rows {
		c.Assert(row[2], Equals, "2")
		c.Assert(row[4], Equals, "Incorrect database name ''.")
	}
}

func (s *testSessionIncSuite) TestEnd(c *C) {
	res := s.tk.MustQueryInc(fmt.Sprintf(`/*%s;--check=1;--backup=0;--ignore-warnings=1;*/
inception_magic_start;use test_inc;create table t1(id int);`, s.getAddr()))
	s.rows = res.Rows()
	c.Assert(s.getAffectedRows(), Equals, 3)

	row := s.rows[s.getAffectedRows()-1]
	c.Assert(row[2], Equals, "2")
	c.Assert(row[4], Equals, "Must end with commit.")
}

func (s *testSessionIncSuite) TestSQLSyntax(c *C) {
	sql := ""

	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false

	sql = "create table t1(id int);create table t1(id int)`;"
	s.testErrorCode(c, sql, session.NewErrf("line 1 column 48 near \"`\" "))

	sql = "alter table t1 add column c1 int comment '123'`;"
	s.testErrorCode(c, sql,
		session.NewErrf("line 1 column 47 near \"`\" "))
}

func (s *testSessionIncSuite) TestCreateTable(c *C) {
	sql := ""

	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false

	// 表存在
	sql = "create table t1(id int);create table t1(id int);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_EXISTS_ERROR, "t1"))

	// 重复列
	sql = "create table test_error_code1 (c1 int, c2 int, c2 int)"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_DUP_FIELDNAME, "c2"))

	// 主键
	config.GetGlobalConfig().Inc.CheckPrimaryKey = true
	sql = ("create table t1(id int);")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_MUST_HAVE_PK, "t1"))

	config.GetGlobalConfig().Inc.CheckPrimaryKey = false

	// 数据类型 警告
	sql = "create table t1(id int,c1 bit);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "bit"))

	sql = "create table t1(id int,c1 enum('red', 'blue', 'black'));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "enum"))

	sql = "create table t1(id int,c1 set('red', 'blue', 'black'));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "set"))

	config.GetGlobalConfig().Inc.EnableTimeStampType = false
	sql = "create table t1(id int,c1 timestamp);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "timestamp"))

	config.GetGlobalConfig().Inc.EnableTimeStampType = true
	sql = "create table t1(id int,c1 timestamp);"
	s.testErrorCode(c, sql)

	// char列建议
	config.GetGlobalConfig().Inc.MaxCharLength = 100
	sql = `create table t1(id int,c1 char(200),c2 char(100));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHAR_TO_VARCHAR_LEN, "c1"))

	config.GetGlobalConfig().Inc.MaxCharLength = 0
	sql = `create table t1(id int,c1 char(200));`
	s.testErrorCode(c, sql)

	// 主键的默认值为函数
	sql = "create table t1(c1 datetime(6) not null default current_timestamp(6) primary key);"
	s.testErrorCode(c, sql)
	//SRID列选项检查
	sql = "create table t1(c1 int not null srid 4326);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_NO_GEOMETRY_DEFAULT, "c1"))
	// 关键字
	config.GetGlobalConfig().Inc.EnableIdentiferKeyword = false
	config.GetGlobalConfig().Inc.CheckIdentifier = true

	sql = ("create table t1(id int, TABLES varchar(20),`c1$` varchar(20),c1234567890123456789012345678901234567890123456789012345678901234567890 varchar(20));")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_IDENT_USE_KEYWORD, "TABLES"),
		session.NewErr(session.ER_INVALID_IDENT, "c1$"),
		session.NewErr(session.ER_TOO_LONG_IDENT, "c1234567890123456789012345678901234567890123456789012345678901234567890"),
	)

	// 列注释
	config.GetGlobalConfig().Inc.CheckColumnComment = true
	sql = "create table t1(c1 varchar(20));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_HAVE_NO_COMMENT, "c1", "t1"))

	config.GetGlobalConfig().Inc.CheckColumnComment = false
	// 表注释
	config.GetGlobalConfig().Inc.CheckTableComment = true
	sql = ("create table t1(c1 varchar(20));")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_MUST_HAVE_COMMENT, "t1"))

	config.GetGlobalConfig().Inc.CheckTableComment = false
	s.mustCheck(c, "create table t1(c1 varchar(20));")

	// 无效默认值
	config.GetGlobalConfig().Inc.EnableEnumSetBit = true
	sql = "create table t1(id int,c1 int default '');"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = "create table t1(id int,c1 varchar(10), c3 int default(c1));"
	if s.DBVersion > 80000 {
		s.testErrorCode(c, sql)
	}

	sql = "create table t1(id int,c1 bit default '0');"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = "create table t1(id int,c1 bit default b'0');"
	s.testErrorCode(c, sql)

	// blob/text字段
	config.GetGlobalConfig().Inc.EnableBlobType = false
	sql = "create table t111(id int,c1 blob, c2 text);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "blob"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "text"),
	)

	config.GetGlobalConfig().Inc.DisableTypes = "blob,text"
	sql = "create table t111(id int,c1 blob, c2 text);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "blob"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "text"),
	)
	config.GetGlobalConfig().Inc.DisableTypes = ""

	config.GetGlobalConfig().Inc.EnableBlobType = true
	sql = ("create table t1(id int,c1 blob not null);")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TEXT_NOT_NULLABLE_ERROR, "c1", "t1"),
	)

	// 指定类型禁用
	config.GetGlobalConfig().Inc.DisableTypes = "bit"
	sql = "create table t1(id int,c1 bit default b'0');"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "bit"))
	config.GetGlobalConfig().Inc.DisableTypes = ""

	config.GetGlobalConfig().Inc.EnableEnumSetBit = false
	sql = "create table t1(id int,c1 bit default b'0');"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "bit"))

	// 检查默认值
	config.GetGlobalConfig().Inc.CheckColumnDefaultValue = true
	sql = "create table t1(c1 varchar(10));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_DEFAULT_ADD_COLUMN, "c1", "t1"))
	config.GetGlobalConfig().Inc.CheckColumnDefaultValue = false

	// 支持innodb引擎
	config.GetGlobalConfig().Inc.EnableSetEngine = true
	config.GetGlobalConfig().Inc.SupportEngine = "innodb"
	s.mustCheck(c, "create table t1(c1 varchar(10))engine = innodb;")

	sql = ("create table t1(c1 varchar(10))engine = myisam;")
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrEngineNotSupport, "innodb"))

	// 时间戳 timestamp默认值
	sql = "create table t1(id int primary key,t1 timestamp default CURRENT_TIMESTAMP,t2 timestamp default CURRENT_TIMESTAMP);"
	s.testErrorCode(c, sql,
		session.NewErrf("Incorrect table definition; there can be only one TIMESTAMP column with CURRENT_TIMESTAMP in DEFAULT or ON UPDATE clause"))

	config.GetGlobalConfig().Inc.CheckTimestampDefault = false
	sql = "create table t1(id int primary key,c1 timestamp default CURRENT_TIMESTAMP,c2 timestamp ON UPDATE CURRENT_TIMESTAMP);"
	if s.explicitDefaultsForTimestamp || !(strings.Contains(s.sqlMode, "TRADITIONAL") ||
		(strings.Contains(s.sqlMode, "STRICT_") && strings.Contains(s.sqlMode, "NO_ZERO_DATE"))) {
		s.testErrorCode(c, sql)
	} else {
		s.testErrorCode(c, sql, session.NewErr(session.ER_INVALID_DEFAULT, "c2"))
	}

	config.GetGlobalConfig().Inc.CheckTimestampDefault = true
	sql = "create table t1(id int primary key,c1 timestamp default CURRENT_TIMESTAMP,c2 timestamp ON UPDATE CURRENT_TIMESTAMP);"
	if s.explicitDefaultsForTimestamp {
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_TIMESTAMP_DEFAULT, "c2"))
	} else if strings.Contains(s.sqlMode, "TRADITIONAL") ||
		(strings.Contains(s.sqlMode, "STRICT_") && strings.Contains(s.sqlMode, "NO_ZERO_DATE")) {
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c2"))
	} else {
		s.testErrorCode(c, sql)
	}

	config.GetGlobalConfig().Inc.CheckTimestampDefault = false
	sql = "create table t1(id int primary key,t1 timestamp default CURRENT_TIMESTAMP,t2 timestamp not null ON UPDATE CURRENT_TIMESTAMP);"
	if s.explicitDefaultsForTimestamp || !(strings.Contains(s.sqlMode, "TRADITIONAL") ||
		(strings.Contains(s.sqlMode, "STRICT_") && strings.Contains(s.sqlMode, "NO_ZERO_DATE"))) {
		s.testErrorCode(c, sql)
	} else {
		s.testErrorCode(c, sql, session.NewErr(session.ER_INVALID_DEFAULT, "t2"))
	}

	sql = "create table t1(id int primary key,t1 timestamp default CURRENT_TIMESTAMP,t2 date default CURRENT_TIMESTAMP);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "t2"))

	// 时间戳 timestamp数量
	config.GetGlobalConfig().Inc.CheckTimestampCount = false
	sql = "create table t1(id int primary key,t1 timestamp default CURRENT_TIMESTAMP,t2 timestamp ON UPDATE CURRENT_TIMESTAMP);"
	if s.explicitDefaultsForTimestamp || !(strings.Contains(s.sqlMode, "TRADITIONAL") ||
		(strings.Contains(s.sqlMode, "STRICT_") && strings.Contains(s.sqlMode, "NO_ZERO_DATE"))) {
		s.testErrorCode(c, sql)
	} else {
		s.testErrorCode(c, sql, session.NewErr(session.ER_INVALID_DEFAULT, "t2"))
	}

	sql = "create table t1(id int primary key,t1 timestamp default CURRENT_TIMESTAMP,t2 timestamp default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.CheckTimestampCount = true
	sql = "create table t1(id int primary key,t1 timestamp default CURRENT_TIMESTAMP,t2 timestamp ON UPDATE CURRENT_TIMESTAMP);"
	if s.explicitDefaultsForTimestamp || !(strings.Contains(s.sqlMode, "TRADITIONAL") ||
		(strings.Contains(s.sqlMode, "STRICT_") && strings.Contains(s.sqlMode, "NO_ZERO_DATE"))) {
		s.testErrorCode(c, sql)
	} else {
		s.testErrorCode(c, sql, session.NewErr(session.ER_INVALID_DEFAULT, "t2"))
	}

	sql = "create table t1(id int primary key,t1 timestamp default CURRENT_TIMESTAMP,t2 timestamp default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);"
	s.testErrorCode(c, sql,
		session.NewErrf("Incorrect table definition; there can be only one TIMESTAMP column with CURRENT_TIMESTAMP in DEFAULT or ON UPDATE clause"))

	sql = "create table test_error_code1 (c1 int, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)"
	s.testErrorCode(c, sql, session.NewErr(session.ER_TOO_LONG_IDENT, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	sql = "create table aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa(a int)"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TOO_LONG_IDENT, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	sql = "create table test_error_code1 (c1 int, c2 int, key aa (c1, c2), key aa (c1))"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_DUP_INDEX, "aa", "test_inc", "test_error_code1"),
		session.NewErr(session.ER_DUP_KEYNAME, "aa"))

	sql = "create table test_error_code1 (c1 int, c2 int, c3 int, key(c_not_exist))"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_NAME_FOR_INDEX, "NULL", "test_error_code1"),
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "test_error_code1.c_not_exist"))

	sql = "create table test_error_code1 (c1 int, c2 int, c3 int, primary key(c_not_exist))"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "test_error_code1.c_not_exist"))

	sql = "create table test_error_code1 (c1 int not null default '')"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = "CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT 2.0 DEFAULT now());"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "a"))

	sql = "CREATE TABLE `t` (`a` double DEFAULT now());"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "a"))

	// 字符集
	config.GetGlobalConfig().Inc.EnableSetCharset = false
	config.GetGlobalConfig().Inc.SupportCharset = ""
	sql = "create table t1(a int) character set utf8;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_CHARSET_MUST_NULL, "t1"))

	config.GetGlobalConfig().Inc.EnableSetCharset = true
	config.GetGlobalConfig().Inc.SupportCharset = "utf8,utf8mb4"
	sql = "create table t1(a int) character set utf8;"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.EnableSetCharset = true
	config.GetGlobalConfig().Inc.EnableSetCollation = true
	config.GetGlobalConfig().Inc.SupportCharset = "utf8,utf8mb4"
	sql = "create table t1(a int) character set latin1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"))

	sql = "create table t1(a int) character set latin123;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"),
		session.NewErr(session.ErrUnknownCharset, "latin123"),
		session.NewErrf("COLLATION '' is not valid for CHARACTER SET 'latin123'!"),
	)

	sql = "create table t1(a int) character set gbk;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"))

	sql = "create table t1(a int) character set gbk collate gbk_bin;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"))

	config.GetGlobalConfig().Inc.SupportCharset = "utf8,utf8mb3,utf8mb4,gbk"

	sql = "create table t1(a int) character set gbk collate gbk_bin;"
	s.testErrorCode(c, sql)
	sql = "create table t1(a int) character set gbk;"
	s.testErrorCode(c, sql)

	sql = "create table t1(a int) character set utf8mb3;"
	if s.DBVersion > 50700 {
		s.testErrorCode(c, sql)
	} else {
		s.testErrorCode(c, sql,
			session.NewErr(session.ErrUnknownCharset, "utf8mb3"))
	}

	// 外键
	sql = "create table test_error_code (a int not null ,b int not null,c int not null, d int not null, foreign key (b, c) references product(id));"
	s.testErrorCode(c, sql,
		// session.NewErr(session.ER_WRONG_NAME_FOR_INDEX, "NULL", "test_error_code"),
		session.NewErr(session.ER_FOREIGN_KEY, "test_error_code"))

	sql = "create table test_error_code (a int not null ,b int not null,c int not null, d int not null, foreign key fk_1(b, c) references product(id));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_FOREIGN_KEY, "test_error_code"))

	sql = "create table test_error_code_2;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_MUST_AT_LEAST_ONE_COLUMN))

	sql = "create table test_error_code_2 (unique(c1));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_MUST_AT_LEAST_ONE_COLUMN))

	sql = "create table test_error_code_2(c1 int, c2 int, c3 int, primary key(c1), primary key(c2));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_MULTIPLE_PRI_KEY))

	sql = "create table test_error_code_2(c1 int, c2 int, c3 int, primary key(c1), key cca(c2));"
	s.testErrorCode(c, sql)

	sql = "create table test_error_code_2(c1 int, c2 int, c3 int, primary key(c1), key(c2));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_NAME_FOR_INDEX, "NULL", "test_error_code_2"))

	config.GetGlobalConfig().Inc.EnableNullIndexName = true

	sql = "create table test_error_code_2(c1 int, c2 int, c3 int, primary key(c1), key(c2));"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.EnableNullIndexName = false

	indexMaxLength := 767
	if s.innodbLargePrefix || s.DBVersion > 80000 {
		indexMaxLength = 3072
	}

	config.GetGlobalConfig().Inc.EnableBlobType = false
	config.GetGlobalConfig().Inc.CheckIndexPrefix = false
	sql = "create table test_error_code_3(pt text ,primary key (pt));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "pt", "text"),
		session.NewErr(session.ER_TOO_LONG_KEY, "PRIMARY", indexMaxLength))

	config.GetGlobalConfig().Inc.EnableBlobType = true
	config.GetGlobalConfig().Inc.EnableColumnCharset = true
	// 索引长度
	sql = "create table test_error_code_3(a text, unique (a(3073)));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_NAME_FOR_INDEX, "NULL", "test_error_code_3"),
		session.NewErr(session.ER_TOO_LONG_KEY, "", indexMaxLength))

	sql = "create table test_error_code_3(c1 int,c2 text, unique uq_1(c1,c2(3069)));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TOO_LONG_KEY, "uq_1", indexMaxLength))

	// ----------------- 索引长度审核 varchar ----------------------
	if s.innodbLargePrefix || s.DBVersion > 80000 {
		sql = "create table test_error_code_3(c1 int primary key,c2 varchar(1024),c3 int, key uq_1(c2,c3)) default charset utf8;"
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_TOO_LONG_KEY, "uq_1", indexMaxLength))

		sql = "create table test_error_code_3(c1 int primary key,c2 varchar(1023),c3 int, key uq_1(c2,c3)) default charset utf8;"
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_TOO_LONG_KEY, "uq_1", indexMaxLength))

		sql = "create table test_error_code_3(c1 int primary key,c2 varchar(1022),c3 int, key uq_1(c2,c3)) default charset utf8;"
		s.testErrorCode(c, sql)
	} else {
		sql = "create table test_error_code_3(c1 int primary key,c2 varchar(256),c3 int, key uq_1(c2,c3)) default charset utf8;"
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_TOO_LONG_KEY, "uq_1", indexMaxLength))

		sql = "create table test_error_code_3(c1 int primary key,c2 varchar(255),c3 int, key uq_1(c2,c3)) default charset utf8;"
		s.testErrorCode(c, sql)

		sql = "create table test_error_code_3(c1 int primary key,c2 varchar(254),c3 int, key uq_1(c2,c3)) default charset utf8;"
		s.testErrorCode(c, sql)
	}

	// sql = "create table test_error_code_3(c1 int,c2 text, unique uq_1(c1,c2(3068)));"
	// if indexMaxLength == 3072 {
	// 	s.testErrorCode(c, sql)
	// } else {
	// 	s.testErrorCode(c, sql,
	// 		session.NewErr(session.ER_TOO_LONG_KEY, "", indexMaxLength))
	// }

	sql = "create table test_error_code_3(pt blob ,primary key (pt));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_BLOB_USED_AS_KEY, "pt"))

	sql = "create table test_error_code_3(`id` int, key `primary`(`id`));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_NAME_FOR_INDEX, "primary", "test_error_code_3"))

	sql = "create table t2(c1.c2 varchar(10));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_TABLE_NAME, "c1"))

	sql = "create table t2 (c1 int default null primary key , age int);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PRIMARY_CANT_HAVE_NULL))

	sql = "create table t2 (id int null primary key , age int);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PRIMARY_CANT_HAVE_NULL))

	sql = "create table t2 (id int default null, age int, primary key(id));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PRIMARY_CANT_HAVE_NULL))

	sql = "create table t2 (id int null, age int, primary key(id));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PRIMARY_CANT_HAVE_NULL))

	sql = `drop table if exists t1;create table t1(
id int auto_increment comment 'test',
crtTime datetime not null DEFAULT CURRENT_TIMESTAMP comment 'test',
uptTime datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'test',
primary key(id)) comment 'test';`
	s.testErrorCode(c, sql)

	// 5.7版本新增计算列
	config.GetGlobalConfig().Inc.EnableJsonType = true
	if s.DBVersion >= 50700 {
		sql = `CREATE TABLE t1(c1 json DEFAULT '{}' COMMENT '日志记录',
	  type tinyint(10) GENERATED ALWAYS AS (json_extract(operate_info, '$.type')) VIRTUAL COMMENT '操作类型')
	  ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_BLOB_CANT_HAVE_DEFAULT, "c1"),
			session.NewErr(session.ER_IDENT_USE_KEYWORD, "type"),
		)

		sql = `CREATE TABLE t1(c1 json DEFAULT NULL COMMENT '日志记录',
	  type1          tinyint(10) GENERATED ALWAYS AS (json_extract(operate_info, '$.type')) VIRTUAL COMMENT '操作类型')
	  ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql)

		sql = `CREATE TABLE t1(c1 json COMMENT '日志记录',
	  type1  tinyint(10) GENERATED ALWAYS AS (json_extract(operate_info, '$.type')) VIRTUAL COMMENT '操作类型')
	  ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql)

		// 计算列移除默认值校验
		config.GetGlobalConfig().Inc.CheckColumnDefaultValue = true

		sql = `CREATE TABLE t1(c1 json DEFAULT '{}' COMMENT '日志记录',
	  type1 tinyint(10) GENERATED ALWAYS AS (json_extract(operate_info, '$.type')) VIRTUAL COMMENT '操作类型')
	  ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_BLOB_CANT_HAVE_DEFAULT, "c1"))

		sql = `CREATE TABLE t1(c1 json DEFAULT NULL COMMENT '日志记录',
	  type1 tinyint(10) GENERATED ALWAYS AS (json_extract(operate_info, '$.type')) VIRTUAL COMMENT '操作类型')
	  ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql)

		sql = `CREATE TABLE t1(c1 json COMMENT '日志记录',
	  type1 tinyint(10) GENERATED ALWAYS AS (json_extract(operate_info, '$.type')) VIRTUAL COMMENT '操作类型')
	  ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql)

		config.GetGlobalConfig().Inc.CheckColumnDefaultValue = false

	} else if s.DBVersion >= 50600 {
		sql = `CREATE TABLE t1(c1 json DEFAULT '{}' COMMENT '日志记录') ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_BLOB_CANT_HAVE_DEFAULT, "c1"))

		sql = `CREATE TABLE t1(c1 json DEFAULT NULL COMMENT '日志记录') ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql)

		sql = `CREATE TABLE t1(c1 json COMMENT '日志记录') ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='xxx';`
		s.testErrorCode(c, sql)
	}

	config.GetGlobalConfig().Inc.EnableNullable = false
	sql = `drop table if exists t1;CREATE TABLE t1(c1 int);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NOT_ALLOWED_NULLABLE, "c1", "t1"))

	if s.DBVersion >= 50700 {
		sql = `CREATE TABLE t1(c1 tinyint(10) GENERATED ALWAYS AS (json_extract(operate_info, '$.type')) VIRTUAL);`
		s.testErrorCode(c, sql)
	}

	config.GetGlobalConfig().Inc.EnableNullable = true
	sql = `drop table if exists t1;CREATE TABLE t1(c1 int);`
	s.testErrorCode(c, sql)

	// 检查必须的字段
	config.GetGlobalConfig().Inc.MustHaveColumns = "c1"
	sql = `drop table if exists t1;CREATE TABLE t1(id int);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_MUST_HAVE_COLUMNS, "c1"))

	sql = `drop table if exists t1;CREATE TABLE t1(c1 int);`
	s.testErrorCode(c, sql)

	// 配置参数时添加多余空格,判断对类型解析是否正确
	config.GetGlobalConfig().Inc.MustHaveColumns = "c1  int,c2 datetime"

	sql = `drop table if exists t1;CREATE TABLE t1(id int);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_MUST_HAVE_COLUMNS, "c1  int,c2 datetime"))

	sql = `drop table if exists t1;CREATE TABLE t1(c1 bigint,c2 int);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_MUST_HAVE_COLUMNS, "c1  int,c2 datetime"))

	sql = `drop table if exists t1;CREATE TABLE t1(c1 int,c2 datetime);`
	s.testErrorCode(c, sql)

	sql = `drop table if exists t1;CREATE TABLE t1(c1 int,c2 DATETIME);`
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.MustHaveColumns = ""

	// 如果表包含以下列，列必须有索引。
	config.GetGlobalConfig().Inc.ColumnsMustHaveIndex = "c1 , c2 int "
	sql = `drop table if exists t1;CREATE TABLE t1(id int, c1 int, c2 int, index idx_c1 (c1));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrColumnsMustHaveIndex, "c2"))

	sql = `drop table if exists t1;CREATE TABLE t1(id int, c1 int, c2 varchar (20), index idx_c1 (c1));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrColumnsMustHaveIndexTypeErr, "c2", "int", "varchar"),
		session.NewErr(session.ErrColumnsMustHaveIndex, "c2"))

	sql = `drop table if exists t1;create table t1(id int,c2 int unique);`
	s.testErrorCode(c, sql)

	sql = `drop table if exists t1;create table t1(c1 int primary key,c2 int unique)`
	s.testErrorCode(c, sql)

	sql = `drop table if exists t1;CREATE TABLE t1(id int, c1 int, c2 int, index idx_1 (c1,c2));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrColumnsMustHaveIndex, "c2"))

	sql = `drop table if exists t1;CREATE TABLE t1(id int,c1 int ,c2 int,index idx_c1(c1),index idx_c2(c2));`
	s.testErrorCode(c, sql)

	sql = `drop table if exists t1;CREATE TABLE t1(id int);ALTER TABLE t1 ADD COLUMN c2 varchar(20);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrColumnsMustHaveIndexTypeErr, "c2", "int", "varchar"),
		session.NewErr(session.ErrColumnsMustHaveIndex, "c2"))

	sql = `drop table if exists t1;CREATE TABLE t1(id int);ALTER TABLE t1 ADD COLUMN c2 int,add index idx_c2(c2);`
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.ColumnsMustHaveIndex = ""

	config.GetGlobalConfig().Inc.CheckInsertField = false
	config.GetGlobalConfig().IncLevel.ErWithInsertField = 0

	// 测试表名大小写
	sql = `drop table if exists t1;CREATE TABLE t1(c1 int);insert into T1 values(1);`
	if s.ignoreCase {
		s.testErrorCode(c, sql)
	} else {
		s.runCheck(sql)
		row := s.rows[s.getAffectedRows()-1]
		c.Assert(row[2], Equals, "2")
		c.Assert(row[4], Equals, "Table 'test_inc.T1' doesn't exist.")
	}

	// 无效默认值
	config.GetGlobalConfig().Inc.CheckAutoIncrementName = true
	sql = `create table t1(c1 int auto_increment primary key,c2 int);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_AUTO_INCR_ID_WARNING, "c1"))

	// 禁止设置存储引擎
	config.GetGlobalConfig().Inc.EnableSetEngine = false
	sql = ("drop table if exists t1;create table t1(c1 varchar(10))engine = innodb;")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_SET_ENGINE, "t1"))

	// 允许设置存储引擎
	config.GetGlobalConfig().Inc.EnableSetEngine = true
	config.GetGlobalConfig().Inc.SupportEngine = "innodb"
	s.mustCheck(c, "drop table if exists t1;create table t1(c1 varchar(10))engine = innodb;")

	// 允许blob,text,json列设置为NOT NULL
	config.GetGlobalConfig().Inc.EnableBlobNotNull = false
	sql = `create table t1(id int auto_increment primary key,c1 blob not null);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TEXT_NOT_NULLABLE_ERROR, "c1", "t1"))

	sql = `create table t1(id int auto_increment primary key,c1 text not null);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TEXT_NOT_NULLABLE_ERROR, "c1", "t1"))

	if s.DBVersion >= 50600 {
		sql = `create table t1(id int auto_increment primary key,c1 json not null);`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_TEXT_NOT_NULLABLE_ERROR, "c1", "t1"))
	}

	config.GetGlobalConfig().Inc.EnableBlobNotNull = true
	sql = `create table t1(id int auto_increment primary key,c1 blob not null);`
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.CheckIndexPrefix = true
	sql = "create table test_error_code_3(a text, unique (a(3073)));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_NAME_FOR_INDEX, "NULL", "test_error_code_3"),
		session.NewErr(session.ER_INDEX_NAME_UNIQ_PREFIX, "",
			config.GetGlobalConfig().Inc.UniqIndexPrefix, "test_error_code_3"),
		session.NewErr(session.ER_TOO_LONG_KEY, "", indexMaxLength))

	sql = "create table test_error_code_3(a text, key (a(3073)));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_NAME_FOR_INDEX, "NULL", "test_error_code_3"),
		session.NewErr(session.ER_INDEX_NAME_IDX_PREFIX, "",
			config.GetGlobalConfig().Inc.IndexPrefix, "test_error_code_3"),
		session.NewErr(session.ER_TOO_LONG_KEY, "", indexMaxLength))

	config.GetGlobalConfig().Inc.IndexPrefix = "idx_,idx1_"
	sql = "create table test_error_code_3(c1 int,c2 int,key idx_1(c1),key idx1_2(c2));"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.IndexPrefix = "idx_,idx1_"
	sql = "create table test_error_code_3(c1 int,c2 int,key idx_1(c1),key idx2_2(c2));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INDEX_NAME_IDX_PREFIX, "idx2_2",
			config.GetGlobalConfig().Inc.IndexPrefix, "test_error_code_3"),
	)

	config.GetGlobalConfig().Inc.TablePrefix = "t_"
	sql = "create table t1(id int primary key);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_PREFIX,
			config.GetGlobalConfig().Inc.TablePrefix))

	// tidb [v3.1.0 版本开始引入]
	sql = "create table t1(a bigint primary key auto_random);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_PREFIX,
			config.GetGlobalConfig().Inc.TablePrefix))
	config.GetGlobalConfig().Inc.TablePrefix = ""

	sql = `create table t1(id int primary key,
				c1 datetime DEFAULT CURRENT_TIMESTAMP(1) ON UPDATE CURRENT_TIMESTAMP);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = `create table t1(id int primary key,
			c1 datetime DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP);`
	s.testErrorCode(c, sql)

	sql = `create table t1(id int primary key,
			c1 datetime DEFAULT CURRENT_TIMESTAMP(7) ON UPDATE CURRENT_TIMESTAMP);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = `CREATE TABLE t1(
			c1 varchar(20) NOT NULL DEFAULT ''
		) ENGINE = InnoDB SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=2;`
	s.testErrorCode(c, sql)

	sql = `create table t1(id int primary key,
			c1 datetime(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP);`
	s.testErrorCode(c, sql,
		session.NewErrf("Invalid ON UPDATE clause for '%s' column.", "c1"))

	config.GetGlobalConfig().Inc.MaxColumnCount = 2
	sql = `create table t1(id int primary key,
			c1 int,c2 int);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrMaxColumnCount, "t1", 2, 3))
	config.GetGlobalConfig().Inc.MaxColumnCount = 0

	sql = `CREATE TABLE t1 (
			id bigint(20) COMMENT 'id',
			c1 double(10) DEFAULT '0',
			c2 DECIMAL(10) DEFAULT '0',
			c3 float(10) DEFAULT '0',
			PRIMARY KEY (id)
			) ENGINE=InnoDB comment = 'test';`
	s.testErrorCode(c, sql,
		session.NewErrf("Please specify the number of digits of type double (column: \"%s\").", "c1"))

	// 检查分区表RANGE类型
	config.GetGlobalConfig().Inc.EnablePartitionTable = true
	sql = `CREATE TABLE t1 (
			c1  VARCHAR(10)
			)
			PARTITION BY RANGE (c1) (
				PARTITION p0 VALUES LESS THAN (1));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrNotAllowedTypeInPartition, "c1"))
	// 检查主键,唯一键必需包含所有分区键
	config.GetGlobalConfig().Inc.EnablePartitionTable = true
	sql = `CREATE TABLE t1 (
			c1  INT,
			c2  INT,
			PRIMARY KEY (c1)
			)
			PARTITION BY RANGE (c2) (
				PARTITION p0 VALUES LESS THAN (1));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrUniqueKeyNeedAllFieldsInPf, "PRIMARY KEY"))

	sql = `CREATE TABLE t1 (
			c1  INT,
			c2  DATE,
			PRIMARY KEY (c1,c2)
			)
			PARTITION BY RANGE (YEAR(c2)) (
				PARTITION p0 VALUES LESS THAN (1));`
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestCreateTableAsSelect(c *C) {
	if s.enforeGtidConsistency {
		sql = "create table t1(id int primary key);create table t11 as select * from t1;"
		s.testErrorCode(c, sql,
			session.NewErrf("Statement violates GTID consistency: CREATE TABLE ... SELECT."))
	} else {
		sql = "create table t1(id int primary key);create table t11 as select * from t1;"
		s.testErrorCode(c, sql)
	}
}

func (s *testSessionIncSuite) TestDropTable(c *C) {
	config.GetGlobalConfig().Inc.EnableDropTable = false
	sql := ""
	sql = "create table t1(id int);drop table t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_DROP_TABLE, "t1"))

	config.GetGlobalConfig().Inc.EnableDropTable = true

	sql = "create table t1(id int);drop table t1;"
	s.testErrorCode(c, sql)

	if s.DBVersion < 80000 {
		s.mustRunExec(c, `drop table if exists t1;
			create table t1(id int auto_increment primary key,c1 int);
			insert into t1(id,c1)values(1,1),(2,2);`)
		config.GetGlobalConfig().Inc.MaxDDLAffectRows = 1
		sql = "drop table t1;"
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_CHANGE_TOO_MUCH_ROWS, "Drop", 2, 1))
	}
}

func (s *testSessionIncSuite) TestAlterTableAddColumn(c *C) {
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false
	config.GetGlobalConfig().Inc.EnableDropTable = true

	s.mustCheck(c, "drop table if exists t1;create table t1(id int);alter table t1 add column c1 int;")

	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c1 int;alter table t1 add column c1 int;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_EXISTED, "t1.c1"))

	s.mustCheck(c, "drop table if exists t1;create table t1(id int);alter table t1 add column c1 int first;alter table t1 add column c2 int after c1;")

	if s.DBVersion > 80000 {
		s.mustCheck(c, "drop table if exists t1;create table t1(id int);alter table t1 add column c1 int visible first;alter table t1 add column c2 int visible after c1;")
	}
	// after 不存在的列
	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c2 int after c1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.c1"))

	// 数据类型 警告
	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c2 bit;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "bit"))

	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c2 enum('red', 'blue', 'black');"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "enum"))

	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c2 set('red', 'blue', 'black');"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "set"))

	// char列建议
	config.GetGlobalConfig().Inc.MaxCharLength = 100
	sql = `drop table if exists t1;create table t1(id int);
		alter table t1 add column c1 char(200);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHAR_TO_VARCHAR_LEN, "c1"))

	// 字符集
	sql = `drop table if exists t1;create table t1(id int);
        alter table t1 add column c1 varchar(20) character set utf8;
        alter table t1 add column c2 varchar(20) COLLATE utf8_bin;`
	s.testManyErrors(c, sql,
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c1"),
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c2"))

	// 关键字
	config.GetGlobalConfig().Inc.EnableIdentiferKeyword = false
	config.GetGlobalConfig().Inc.CheckIdentifier = true

	sql = ("drop table if exists t1;create table t1(id int);alter table t1 add column TABLES varchar(20);alter table t1 add column `c1$` varchar(20);alter table t1 add column c1234567890123456789012345678901234567890123456789012345678901234567890 varchar(20);")
	s.testManyErrors(c, sql,
		session.NewErr(session.ER_IDENT_USE_KEYWORD, "TABLES"),
		session.NewErr(session.ER_INVALID_IDENT, "c1$"),
		session.NewErr(session.ER_TOO_LONG_IDENT, "c1234567890123456789012345678901234567890123456789012345678901234567890"),
	)

	// 列注释
	config.GetGlobalConfig().Inc.CheckColumnComment = true
	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c1 varchar(20);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_HAVE_NO_COMMENT, "c1", "t1"))
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	//列默认值表达式检查
	sql = ("drop table if exists t1;create table t1(id int);alter table t1 add column c1 int default (id);")
	if s.DBVersion >= 80000 {
		s.testErrorCode(c, sql)
	}
	// 无效默认值
	sql = ("drop table if exists t1;create table t1(id int);alter table t1 add column c1 int default '';")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c1 int default '';"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	config.GetGlobalConfig().Inc.EnableEnumSetBit = true
	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c1 bit default '0';"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	// blob/text字段
	config.GetGlobalConfig().Inc.EnableBlobType = false
	sql = ("drop table if exists t1;create table t1(id int);alter table t1 add column c1 blob;alter table t1 add column c2 text;")
	s.testManyErrors(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "blob"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "text"),
	)

	config.GetGlobalConfig().Inc.EnableBlobType = true
	sql = ("drop table if exists t1;create table t1(id int);alter table t1 add column c1 blob not null;")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TEXT_NOT_NULLABLE_ERROR, "c1", "t1"),
	)

	// 检查默认值
	config.GetGlobalConfig().Inc.CheckColumnDefaultValue = true
	sql = "drop table if exists t1;create table t1(id int);alter table t1 add column c1 varchar(10);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_DEFAULT_ADD_COLUMN, "c1", "t1"))
	config.GetGlobalConfig().Inc.CheckColumnDefaultValue = false

	sql = "drop table if exists t1;create table t1(id int primary key , age int);"
	s.testErrorCode(c, sql)

	// // add column
	sql = "drop table if exists t1;create table t1 (c1 int primary key);alter table t1 add column c1 int"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_EXISTED, "t1.c1"))

	sql = "drop table if exists t1;create table t1 (c1 int primary key);alter table t1 add column aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TOO_LONG_IDENT, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	sql = "drop table if exists t1;alter table t1 comment 'test comment'"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_NOT_EXISTED_ERROR, "test_inc.t1"))

	sql = "drop table if exists t1;create table t1 (c1 int primary key);alter table t1 add column `a ` int ;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_IDENT, "a "),
		session.NewErr(session.ER_WRONG_COLUMN_NAME, "a "))

	sql = "drop table if exists t1;create table t1 (c1 int primary key);alter table t1 add column c2 int on update current_timestamp;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_ON_UPDATE, "c2"))

	sql = "drop table if exists t1;create table t1(c2 int on update current_timestamp);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_ON_UPDATE, "c2"))

	if s.DBVersion >= 50600 {
		config.GetGlobalConfig().Inc.EnableJsonType = true
		sql = "drop table if exists t1;create table t1 (c1 int primary key);alter table t1 add c2 json;"
		s.testErrorCode(c, sql)
		config.GetGlobalConfig().Inc.EnableJsonType = false
		sql = "drop table if exists t1;create table t1 (c1 int primary key);alter table t1 add c2 json;"
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "json"))
	}

	sql = "drop table if exists t1;create table t1 (id int primary key);alter table t1 add column (c1 int,c2 varchar(20));"
	s.testErrorCode(c, sql)

	if s.DBVersion > 80000 {
		sql = "drop table if exists t1;create table t1 (id int primary key);alter table t1 add column (c1 int,c2 varchar(20) visible);"
		s.testErrorCode(c, sql)
	}
	// 指定特殊选项
	sql = "drop table if exists t1;create table t1 (id int primary key);alter table t1 add column c1 int,ALGORITHM=INPLACE, LOCK=NONE;"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.CheckIdentifier = false
	// 特殊字符
	sql = "drop table if exists `t3!@#$^&*()`;create table `t3!@#$^&*()`(id int primary key);alter table `t3!@#$^&*()` add column `c3!@#$^&*()2` int comment '123';"
	s.testErrorCode(c, sql)

	sql = "drop table if exists t1;create table t1(id int primary key);alter table t1 add column c1 int primary key;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_DUP_INDEX, "PRIMARY", "test_inc", "t1"))

	sql = "drop table if exists t1;create table t1(id int,key c1(id));alter table t1 add column c1 int unique;"
	s.testErrorCode(c, sql)

	sql = `drop table if exists t1;
			create table t1(id int,key c1(id));
			alter table t1 add column c1 int unique;
			alter table t1 add index c1(c1);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_DUP_INDEX, "c1", "test_inc", "t1"))

	config.GetGlobalConfig().Inc.MaxVarcharLength = 100
	sql = `drop table if exists t1;
			create table t1(id int,c1 varchar(200));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrMaxVarcharLength, "c1", 100))

	sql = `drop table if exists t1;
			create table t1(id int,key c1(id));
			alter table t1 add column c1 varchar(1000);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrMaxVarcharLength, "c1", 100))

	sql = `drop table if exists t1;
		create table t1(id int,c1 char(10));
		alter table t1 modify column c1 varchar(1000);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrMaxVarcharLength, "c1", 100))

	sql = `drop table if exists t1;
		create table t1(id int,c1 char(10));
		alter table t1 modify column c1 char(100);`
	s.testErrorCode(c, sql)

	// 	ALTER TABLE t1 ADD FULLTEXT INDEX ft_index (c1) WITH PARSER ngram;

	// alter table t1 add index ix_1(c1) /*!50100 WITH PARSER `ngram` */;
	sql = `drop table if exists t1;
		create table t1(id int,c1 char(10));
		ALTER TABLE t1 ADD FULLTEXT INDEX ft_index (c1) WITH PARSER ngram;`
	s.testErrorCode(c, sql)

	sql = `drop table if exists t1;
		create table t1(id int,c1 char(10));
		alter table t1 add index ix_1(c1) /*!50100 WITH PARSER ngram */;`
	s.testErrorCode(c, sql,
		session.NewErrf("WITH PARSER option can be used only with FULLTEXT indexes."))

	config.GetGlobalConfig().Inc.MaxColumnCount = 2
	sql = `drop table if exists t1;
	create table t1(id int,c1 char(10));
	alter table t1 add column c2 int;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrMaxColumnCount, "t1", 2, 3))
	config.GetGlobalConfig().Inc.MaxColumnCount = 0

	sql = `drop table if exists t1;
		create table t1(id int,c1 char(10));
		alter table t1 add column cc char(20) unique key;`
	s.testErrorCode(c, sql)
	// 检查自增属性
	sql = `drop table if exists t1;
		create table t1(id int,c1 char(10));
		alter table t1 add column c2 int auto_increment;`
	s.testErrorCode(c, sql,
		session.NewErrf("Incorrect table definition; there can be only one auto column and it must be defined as a key."))

	sql = `drop table if exists t1;
		create table t1(id int);
		alter table t1 add column c1 datetime default current_timestamp;`
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestAlterTableRenameColumn(c *C) {
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false
	config.GetGlobalConfig().Inc.EnableDropTable = true

	s.mustRunExec(c, "drop table if exists t1;create table t1(id int primary key,c1 int);")

	if s.DBVersion < 80000 {
		return
	}

	sql = `alter table t1 rename column c1 to c2;`
	s.testErrorCode(c, sql)

	sql = `alter table t1 rename column c1 to id;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_EXISTED, "t1.id"))

	sql = `alter table t1 rename column c2 to id;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.c2"))
}

func (s *testSessionIncSuite) TestAlterTableAlterColumn(c *C) {
	sql = ("create table t1(id int);alter table t1 alter column id set default '';")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "id"))

	s.mustCheck(c, "create table t1(id int);alter table t1 alter column id set default '1';")

	s.mustCheck(c, "create table t1(id int);alter table t1 alter column id drop default ;alter table t1 alter column id set default '1';")

	if s.DBVersion > 80000 {
		s.mustCheck(c, "create table t1(id int);alter table t1 alter column id set visible;")
	}
}

func (s *testSessionIncSuite) TestAlterTableModifyColumn(c *C) {
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false

	s.runCheck("create table t1(id int,c1 int);alter table t1 modify column c1 int first;")
	c.Assert(s.getAffectedRows(), GreaterEqual, 2)
	for _, row := range s.rows {
		c.Assert(row[2], Not(Equals), "2")
	}

	s.runCheck("create table t1(id int,c1 int);alter table t1 modify column id int after c1;")
	c.Assert(s.getAffectedRows(), GreaterEqual, 2)
	for _, row := range s.rows {
		c.Assert(row[2], Not(Equals), "2")
	}

	if s.DBVersion > 80000 {
		s.runCheck("create table t1(id int,c1 int);alter table t1 modify column c1 int visible first;")
		c.Assert(s.getAffectedRows(), GreaterEqual, 2)
		for _, row := range s.rows {
			c.Assert(row[2], Not(Equals), "2")
		}

		s.runCheck("create table t1(id int,c1 int);alter table t1 modify column id int visible after c1;")
		c.Assert(s.getAffectedRows(), GreaterEqual, 2)
		for _, row := range s.rows {
			c.Assert(row[2], Not(Equals), "2")
		}
	}
	// after 不存在的列
	sql = "create table t1(id int);alter table t1 modify column c1 int after id;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.c1"))

	sql = "create table t1(id int,c1 int);alter table t1 modify column c1 int after id1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.id1"))

	// 数据类型 警告
	sql = "create table t1(id bit);alter table t1 modify column id bit;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "id", "bit"))

	sql = "create table t1(id enum('red', 'blue'));alter table t1 modify column id enum('red', 'blue', 'black');"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "id", "enum"))

	sql = "create table t1(id set('red'));alter table t1 modify column id set('red', 'blue', 'black');"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "id", "set"))

	// char列建议
	config.GetGlobalConfig().Inc.MaxCharLength = 100
	sql = `create table t1(id int,c1 char(10));
		alter table t1 modify column c1 char(200);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHAR_TO_VARCHAR_LEN, "c1"))

	// 字符集
	sql = `create table t1(id int,c1 varchar(20));
		alter table t1 modify column c1 varchar(20) character set utf8;
		alter table t1 modify column c1 varchar(20) COLLATE utf8_bin;`
	s.testManyErrors(c, sql,
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c1"),
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c1"))

	// 列注释
	config.GetGlobalConfig().Inc.CheckColumnComment = true
	sql = ("create table t1(id int,c1 varchar(10));alter table t1 modify column c1 varchar(20);")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_HAVE_NO_COMMENT, "c1", "t1"),
	)

	config.GetGlobalConfig().Inc.CheckColumnComment = false

	// 无效默认值
	sql = ("create table t1(id int,c1 int);alter table t1 modify column c1 int default '';")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	// blob/text字段
	config.GetGlobalConfig().Inc.EnableBlobType = false
	config.GetGlobalConfig().Inc.CheckColumnTypeChange = false
	sql = ("create table t1(id int,c1 varchar(10));alter table t1 modify column c1 blob;")
	s.testManyErrors(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "blob"),
	)

	config.GetGlobalConfig().Inc.EnableBlobType = true
	sql = ("create table t1(id int,c1 blob);alter table t1 modify column c1 blob not null;")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TEXT_NOT_NULLABLE_ERROR, "c1", "t1"),
	)

	// 检查默认值
	config.GetGlobalConfig().Inc.CheckColumnDefaultValue = true
	sql = "create table t1(id int,c1 varchar(5));alter table t1 modify column c1 varchar(10);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_DEFAULT_ADD_COLUMN, "c1", "t1"))
	config.GetGlobalConfig().Inc.CheckColumnDefaultValue = false

	// 变更类型
	config.GetGlobalConfig().Inc.CheckColumnTypeChange = false
	sql = "create table t1(c1 int,c1 int);alter table t1 modify column c1 varchar(10);"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = false
	sql = "create table t1(c1 int,c1 json);alter table t1 modify column c1 varchar(10);"
	s.testErrorCode(c, sql)

	// ----------------- 列类型变更 -----------------
	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 int);alter table t1 modify column c1 varchar(10);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "int(11)", "varchar(10)"))

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 smallint);alter table t1 modify column c1 int;"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 int);alter table t1 modify column c1 smallint;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "int(11)", "smallint(6)"))

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 int);alter table t1 modify column c1 smallint(5);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "int(11)", "smallint(5)"))

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 varchar(100));alter table t1 modify column c1 varchar(20);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "varchar(100)", "varchar(20)"))

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 float);alter table t1 modify column c1 double;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "float", "double"))

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 decimal(10,4));alter table t1 modify column c1 int;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "decimal(10,4)", "int(11)"))

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 decimal(10,4));alter table t1 modify column c1 decimal(8,4);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "decimal(10,4)", "decimal(8,4)"))

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 decimal(10,4));alter table t1 modify column c1 decimal(12,4);"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.CheckColumnTypeChange = true
	sql = "create table t1(c1 json);alter table t1 modify column c1 varchar(10);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "json", "varchar(10)"))

	// 变更长度时不影响(仅长度变小时警告)
	sql = "create table t1(c1 char(100));alter table t1 modify column c1 char(20);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "char(100)", "char(20)"))

	sql = "create table t1(c1 varchar(100));alter table t1 modify column c1 char(10);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHANGE_COLUMN_TYPE, "t1.c1", "varchar(100)", "char(10)"))

	sql = "create table t1(id int primary key,t1 timestamp default CURRENT_TIMESTAMP,t2 timestamp ON UPDATE CURRENT_TIMESTAMP);"
	if s.explicitDefaultsForTimestamp || !(strings.Contains(s.sqlMode, "TRADITIONAL") ||
		(strings.Contains(s.sqlMode, "STRICT_") && strings.Contains(s.sqlMode, "NO_ZERO_DATE"))) {
		s.testErrorCode(c, sql)
	} else {
		s.testErrorCode(c, sql, session.NewErr(session.ER_INVALID_DEFAULT, "t2"))
	}

	// modify column
	sql = "create table t1(id int primary key,c1 int);alter table t1 modify testx.t1.c1 int"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_DB_NAME, "testx"))

	sql = "create table t1(id int primary key,c1 int);alter table t1 modify t.c1 int"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_TABLE_NAME, "t"))

	config.GetGlobalConfig().Inc.CheckColumnPositionChange = true
	sql = "create table t1(id int primary key,c1 int,c2 int);alter table t1 add column c3 int first"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErCantChangeColumnPosition, "t1.c3"))
	sql = "create table t1(id int primary key,c1 int,c2 int);alter table t1 add column c3 int after c1"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErCantChangeColumnPosition, "t1.c3"))

	sql = "create table t1(id int primary key,c1 int,c2 int);alter table t1 modify column c1 int after c2"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErCantChangeColumnPosition, "t1.c1"))

	sql = "create table t1(id int primary key,c1 int,c2 int);alter table t1 change column c1 c3 int after id"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErCantChangeColumnPosition, "t1.c3"))

	config.GetGlobalConfig().Inc.CheckColumnPositionChange = false

	// modify column后,列信息更新
	s.mustRunExec(c, "drop table if exists t1;create table t1(id int not null,c1 int);")
	sql = "alter table t1 add primary key(id,c1);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PRIMARY_CANT_HAVE_NULL))

	sql = "alter table t1 modify c1 int not null;alter table t1 add primary key(id,c1);"
	s.testErrorCode(c, sql)

	s.mustRunExec(c, "drop table if exists t1;create table t1(id int not null,c1 varchar(20), c2 varchar(20));")
	sql = "alter table t1 modify c2 varchar(20) default (c1);"
	if s.DBVersion > 80000 {
		s.testErrorCode(c, sql)
	}

	config.GetGlobalConfig().Inc.EnableIdentiferKeyword = false
	s.mustRunExec(c, "drop table if exists t1;create table t1(id int not null,`alter` int);")
	sql = "alter table t1 modify `alter` bigint;"
	s.testErrorCode(c, sql)

	s.mustRunExec(c, "drop table if exists t1;create table t1(c1 varchar(10));")
	sql = "alter table t1 modify column c1 varchar(10) not null default '';"
	s.testErrorCode(c, sql)

	s.mustRunExec(c, "drop table if exists t1;create table t1(c1 datetime);")
	sql = "alter table t1 modify column c1 datetime not null default current_timestamp;"
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestAlterTableChangeColumn(c *C) {
	var sql string
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false

	s.mustCheck(c, "create table t1(id int,c1 int);alter table t1 modify column c1 int first;")

	s.mustCheck(c, "create table t1(id int,c1 int);alter table t1 modify column id int after c1;")

	if s.DBVersion > 80000 {
		s.mustCheck(c, "create table t1(id int,c1 int);alter table t1 modify column c1 int visible first;")
		s.mustCheck(c, "create table t1(id int,c1 int);alter table t1 modify column id int visible after c1;")
	}
	config.GetGlobalConfig().Inc.EnableChangeColumn = false

	sql = "create table t1(id int primary key,c1 int,c2 int);alter table t1 change column c1 c3 int after id"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErCantChangeColumn, "c1"))

	config.GetGlobalConfig().Inc.EnableChangeColumn = true

	config.GetGlobalConfig().Inc.EnableIdentiferKeyword = false
	s.mustRunExec(c, "drop table if exists t1;create table t1(id int not null,`alter` int);")
	sql = "alter table t1 change `alter` `alter` bigint;"
	s.testErrorCode(c, sql)
	sql = "alter table t1 change `alter` `delete` bigint;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_IDENT_USE_KEYWORD, "delete"))

	sql = "create table t1(id int primary key,c1 varchar(10),c2 varchar(10));alter table t1 change column c2 c2 varchar(10) default(c1)"
	if s.DBVersion > 80000 {
		s.testErrorCode(c, sql)
	}

	sql = "create table t1(id int primary key,c1 varchar(10),c2 varchar(10));alter table t1 change column c2 c3 varchar(10) default(c1)"
	if s.DBVersion > 80000 {
		s.testErrorCode(c, sql)
	}
}

func (s *testSessionIncSuite) TestAlterTableDropColumn(c *C) {
	sql := ""
	sql = "create table t1(id int,c1 int);alter table t1 drop column c2;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.c2"))

	sql = "create table t1(id int,c1 int);alter table t1 drop column c1;"
	s.testErrorCode(c, sql)

	// // drop column
	sql = "create table t2 (id int null);alter table t2 drop c1"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t2.c1"))

	sql = "create table t2 (id int null);alter table t2 drop id;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCantRemoveAllFields))

	sql = "create table t2 (id int null, c1 varchar(20), c2 varchar(20) default(c1));alter table t2 drop c1;"
	if s.DBVersion > 80000 {
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_CANT_DROP_DEPENDENCY_COLUMN, "c1", "t1"))
	}
}

func (s *testSessionIncSuite) TestInsert(c *C) {
	config.GetGlobalConfig().Inc.CheckInsertField = false
	config.GetGlobalConfig().IncLevel.ErWithInsertField = 0

	// 表不存在
	sql = "insert into t1 values(1,1);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_NOT_EXISTED_ERROR, "test_inc.t1"))

	// 列数不匹配
	sql = "create table t1(id int,c1 int);insert into t1(id) values(1,1);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_VALUE_COUNT_ON_ROW, 1))

	sql = "create table t1(id int,c1 int);insert into t1(id) values(1),(2,1);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_VALUE_COUNT_ON_ROW, 2))

	sql = "create table t1(id int,c1 int not null);insert into t1(id,c1) select 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WRONG_VALUE_COUNT_ON_ROW, 1))

	// 列重复
	sql = "create table t1(id int,c1 int);insert into t1(id,id) values(1,1);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_FIELD_SPECIFIED_TWICE, "id", "t1"))

	sql = "create table t1(id int,c1 int);insert into t1(id,id) select 1,1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_FIELD_SPECIFIED_TWICE, "id", "t1"))

	// 字段警告
	config.GetGlobalConfig().Inc.CheckInsertField = true
	config.GetGlobalConfig().IncLevel.ErWithInsertField = 1
	sql = "create table t1(id int,c1 int);insert into t1 values(1,1);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_INSERT_FIELD))
	config.GetGlobalConfig().Inc.CheckInsertField = false
	config.GetGlobalConfig().IncLevel.ErWithInsertField = 0

	sql = "create table t1(id int,c1 int);insert into t1(id) values();"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_INSERT_VALUES))

	// 列不允许为空
	sql = "create table t1(id int,c1 int not null);insert into t1(id,c1) values(1,null);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_BAD_NULL_ERROR, "test_inc.t1.c1", 1))

	sql = "create table t1(id int,c1 int not null default 1);insert into t1(id,c1) values(1,null);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_BAD_NULL_ERROR, "test_inc.t1.c1", 1))

	// insert select 表不存在
	sql = "create table t1(id int,c1 int );insert into t1(id,c1) select 1,null from t2;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_NOT_EXISTED_ERROR, "test_inc.t2"))

	s.mustRunExec(c, "create table t1(id int,c1 int );")

	config.GetGlobalConfig().Inc.CheckDMLWhere = true
	sql = "insert into t1(id,c1) select 1,null from t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NO_WHERE_CONDITION))
	config.GetGlobalConfig().Inc.CheckDMLWhere = false

	// limit
	config.GetGlobalConfig().Inc.CheckDMLLimit = true
	sql = "insert into t1(id,c1) select 1,null from t1 limit 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_LIMIT_CONDITION))
	config.GetGlobalConfig().Inc.CheckDMLLimit = false

	// order by rand()
	// config.GetGlobalConfig().Inc.CheckDMLOrderBy = true
	sql = "insert into t1(id,c1) select 1,null from t1 order by rand();"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_ORDERY_BY_RAND))

	// config.GetGlobalConfig().Inc.CheckDMLOrderBy = false

	// 受影响行数
	s.runCheck("insert into t1 values(1,1),(2,2);")
	s.testAffectedRows(c, 2)

	s.runCheck("insert into t1(id,c1) select 1,null;")
	s.testAffectedRows(c, 1)

	s.mustRunExec(c, "drop table if exists t1; create table t1(c1 char(100) not null);")

	sql = "create table t1(c1 char(100) not null);insert into t1(c1) values(null);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_BAD_NULL_ERROR, "test_inc.t1.c1", 1))

	sql = "create table t1(c1 char(100) not null);insert into t1(c1) select t1.c1 from t1 inner join t1 on t1.id=t1.id;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrNonUniqTable, "t1"))

	// 由于是否报错依赖于实际的mysql版本，所以暂时忽略
	// sql = "create table t1(c1 char(100) not null);insert into t1(c1) select t1.c1 from t1 limit 1 union all select t1.c1 from t1;"
	// s.testErrorCode(c, sql,
	// 	session.NewErr(session.ErrWrongUsage, "UNION", "LIMIT"))

	// sql = "create table t1(c1 char(100) not null);insert into t1(c1) select t1.c1 from t1 order by 1 union all select t1.c1 from t1;"
	// s.testErrorCode(c, sql,
	// 	session.NewErr(session.ErrWrongUsage, "UNION", "ORDER BY"))

	// insert 行数
	config.GetGlobalConfig().Inc.MaxInsertRows = 1

	sql = `drop table if exists t1;create table t1(id int);
insert into t1 values(1);`
	s.testErrorCode(c, sql)
	s.testAffectedRows(c, 1)

	sql = `drop table if exists t1;create table t1(id int);
insert into t1 values(1),(2);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INSERT_TOO_MUCH_ROWS, 2, 1))

	config.GetGlobalConfig().Inc.MaxInsertRows = 3
	sql = `drop table if exists t1;create table t1(id int);
insert into t1 values(1),(2),(3);`

	s.testErrorCode(c, sql)
	s.testAffectedRows(c, 3)

	s.mustRunExec(c, "drop table if exists t1;create table t1(id int);")
	sql = `drop table if exists t2;create table t2 like t1;
insert into t2 select id from t1;`
	s.testErrorCode(c, sql)
	if s.realRowCount {
		s.testAffectedRows(c, 0)
	} else {
		s.testAffectedRows(c, 1)
	}

	config.GetGlobalConfig().Inc.EnableSelectStar = true
	s.mustRunExec(c, "drop table if exists tt1;create table tt1(id int,c1 int);insert into tt1 values(1,1);")
	sql = `insert into tt1 select a.* from tt1 a inner join tt1 b on a.id=b.id;`
	s.testErrorCode(c, sql)

	sql = `insert into tt1 select a.* from tt1 a inner join tt1 b on a.id=b.id;`
	s.testErrorCode(c, sql)

	sql = `insert into tt1 select A.* from (select * from tt1) a inner join tt1 b on a.id=b.id;`
	s.testErrorCode(c, sql)

	sql = `insert into tt1 select A.* from (select c2.* from tt1 c1 inner join tt1 c2 on c1.id=c2.id) a inner join test_inc.tt1 b on a.id=b.id;`
	s.testErrorCode(c, sql)

	sql = `insert into test_inc.tt1 select A.* from (select * from tt1 c1 union all select * from tt1 c2) a inner join test_inc.tt1 b on a.id=b.id;`
	s.testErrorCode(c, sql)

	sql = `insert into test_inc.tt1 select test_inc.B.* from tt1 a inner join test_inc.tt1 b on a.id=b.id;`
	s.testErrorCode(c, sql)

	sql = `insert into test_inc.tt1 select * from test_inc.tt1 ;`
	s.testErrorCode(c, sql)

	sql = `insert into test_inc.tt1(id)values(c1);`
	s.testErrorCode(c, sql)

	sql = `insert into test_inc.tt1(id)values(now);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "now"))

	sql = `insert into t1(id) values(nullif(a,'123'));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "a"))

	sql = `insert into t1(id) values(now);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "now"))

	sql = `insert into t1(id) values(now());`
	s.testErrorCode(c, sql)

	sql = `insert into t1(id) values(max(1));`
	s.testErrorCode(c, sql)

	sql = `insert into t1(id) values(max(a));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "a"))

	sql = `insert into t1(id) values(abs(-1));`
	s.testErrorCode(c, sql)

	sql = `insert into t1(id) values(cast(a as signed));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "a"))

	sql = `drop table if exists tt1;create table tt1(id int,c1 int);insert into tt1(id) select max(id) from tt1 where id in (select id1 from tt1);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "id1"))

	sql = `drop table if exists tt1;create table tt1(id int,c1 int);
	drop table if exists t1;create table t1(id int primary key,c1 int);
	insert into tt1(id)
		select s1.id from t1 as s1 inner join t1 as s2 on s1.c1 = s2.c1 where s1.id > s2.id;`
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.EnableSelectStar = false
	sql = `drop table if exists tt1;create table tt1(id int,c1 int);insert into tt1 select * from tt1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_SELECT_ONLY_STAR))

	// datetime format 验证
	sql = `drop table if exists t1;
		create table t1(id int auto_increment primary key,c1 date,c2 time,c3 datetime,c4 timestamp);`
	s.mustRunExec(c, sql)

	sql = `insert into t1(c1) values('2020-1-32');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrIncorrectDateTimeValue, "2020-1-32", "test_inc.t1.c1"))

	sql = `insert into t1(c2) values('10:70');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrIncorrectDateTimeValue, "10:70", "test_inc.t1.c2"))

	sql = `insert into t1(c3) values('2020-1-32');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrIncorrectDateTimeValue, "2020-1-32", "test_inc.t1.c3"))

	sql = `insert into t1(c4) values('2020-1-32');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrIncorrectDateTimeValue, "2020-1-32", "test_inc.t1.c4"))

}

func (s *testSessionIncSuite) TestSelect(c *C) {
	config.GetGlobalConfig().Inc.EnableSelectStar = false

	s.mustRunExec(c, "drop table if exists t1;create table t1(id int,c1 integer);")
	sql = `select * from t1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_SELECT_ONLY_STAR))

	sql = `select id,c1 from t1 union all select * from t1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_SELECT_ONLY_STAR))

	// 列隐式转换审核
	config.GetGlobalConfig().Inc.CheckImplicitTypeConversion = true
	sql = `select id,c1 from t1 where c1 ="1";`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrImplicitTypeConversion, "t1", "c1", "int"))

	sql = `select id from t1 order by id;`
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestPartition(c *C) {
	config.GetGlobalConfig().Inc.EnablePartitionTable = false

	s.mustRunExec(c, "drop table if exists test_partition;")
	sql = `create table test_partition(
		id int unsigned not null auto_increment comment 'id',
		a varchar(10) comment 'a',
		buss_day varchar(8) comment 'day',
		primary key (id,buss_day)
	) comment '分区表'
	partition by list columns (buss_day)
	(
		partition p1 values in('20200101'),
		partition p2 values in('20200102'),
		partition p3 values in('20200103')
		)`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PARTITION_NOT_ALLOWED))

	sql = `create table test_partition(
		id int unsigned not null auto_increment comment 'id',
		a varchar(10) comment 'a',
		buss_day varchar(8) comment 'day',
		primary key (id,buss_day)
	) comment '分区表'
	partition by list columns (buss_day)
	(
		partition p1 values in('20200101'),
		partition p2 values in('20200102'),
		partition p3 values in('20200103')
		);
		alter table test_partition add partition (
			partition p4 values in('20200104'),
			partition p5 values in('20200105'),
			partition p6 values in('20200106')
			);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PARTITION_NOT_ALLOWED))

	//	----------- enable partition -----------
	config.GetGlobalConfig().Inc.EnablePartitionTable = true
	sql = `create table test_partition(
			id int unsigned not null auto_increment comment 'id',
			a varchar(10) comment 'a',
			buss_day varchar(8) comment 'day',
			primary key (id,buss_day)
		) comment '分区表'
		partition by list columns (buss_day)
		(
			partition p1 values in('20200101'),
			partition p2 values in('20200102'),
			partition p3 values in('20200103')
			)`
	s.testErrorCode(c, sql)
	s.mustRunExec(c, sql)

	sql = `alter table test_partition add partition (
			partition p4 values in('20200104'),
			partition p5 values in('20200105'),
			partition p6 values in('20200106')
			);`
	s.testErrorCode(c, sql)
	s.mustRunExec(c, sql)

	sql = `alter table test_partition drop partition p5,p6;`
	s.testErrorCode(c, sql)
	s.mustRunExec(c, sql)

	sql = `alter table test_partition drop partition p5,p6;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrPartitionNotExisted, "p5"),
		session.NewErr(session.ErrPartitionNotExisted, "p6"))

	sql = `CREATE TABLE my_range_datetime(
    id INT,
    hiredate DATETIME
)
PARTITION BY RANGE (TO_DAYS(hiredate) ) (
    PARTITION p1 VALUES LESS THAN ( TO_DAYS('20171202') ),
    PARTITION p2 VALUES LESS THAN ( TO_DAYS('20171203') ),
    PARTITION p3 VALUES LESS THAN ( TO_DAYS('20171204') ),
    PARTITION p4 VALUES LESS THAN ( TO_DAYS('20171205') ),
    PARTITION p5 VALUES LESS THAN ( TO_DAYS('20171206') ),
    PARTITION p6 VALUES LESS THAN ( TO_DAYS('20171207') ),
    PARTITION p7 VALUES LESS THAN ( TO_DAYS('20171208') ),
    PARTITION p8 VALUES LESS THAN ( TO_DAYS('20171209') ),
    PARTITION p9 VALUES LESS THAN ( TO_DAYS('20171210') ),
    PARTITION p10 VALUES LESS THAN ( TO_DAYS('20171211') ),
    PARTITION p11 VALUES LESS THAN (MAXVALUE)
);`
	s.mustRunExec(c, sql)

	sql = `create table t_list(a int(11),b int(11))
		partition by list (b)(
		partition p0 values in (1,3,5,7,9),
		partition p1 values in (2,4,6,8,0));`
	s.mustRunExec(c, sql)

	sql = `CREATE TABLE my_member (
		id INT NOT NULL,
		fname VARCHAR(30),
		lname VARCHAR(30),
		created DATE NOT NULL DEFAULT '1970-01-01',
		separated DATE NOT NULL DEFAULT '9999-12-31',
		job_code INT,
		store_id INT
	)
	PARTITION BY HASH(id)
	PARTITIONS 4;`
	s.mustRunExec(c, sql)

	sql = `CREATE TABLE my_members (
		id INT NOT NULL,
		fname VARCHAR(30),
		lname VARCHAR(30),
		hired DATE NOT NULL DEFAULT '1970-01-01',
		separated DATE NOT NULL DEFAULT '9999-12-31',
		job_code INT,
		store_id INT
	)
	PARTITION BY LINEAR HASH( id )
	PARTITIONS 4;`
	s.mustRunExec(c, sql)

	sql = `CREATE TABLE k1 (
		id INT NOT NULL PRIMARY KEY,
		name VARCHAR(20)
	)
	PARTITION BY KEY()
	PARTITIONS 2;`
	s.mustRunExec(c, sql)

	sql = `CREATE TABLE tm1 (
		s1 CHAR(32)
	)
	PARTITION BY KEY(s1)
	PARTITIONS 10;`
	s.mustRunExec(c, sql)

	sql = `CREATE TABLE customer_login_log (
		customer_id int(10) unsigned NOT NULL COMMENT '登录用户ID',
		login_time DATETIME NOT NULL COMMENT '用户登录时间',
		login_ip int(10) unsigned NOT NULL COMMENT '登录IP',
		login_type tinyint(4) NOT NULL COMMENT '登录类型:0未成功 1成功'
	  ) ENGINE=InnoDB
	  PARTITION BY RANGE (YEAR(login_time))(
	  PARTITION p0 VALUES LESS THAN (2017),
	  PARTITION p1 VALUES LESS THAN (2018),
	  PARTITION p2 VALUES LESS THAN (2019)
	  );
	  CREATE TABLE arch_customer_login_log (
        customer_id INT unsigned NOT NULL COMMENT '登录用户ID',
        login_time DATETIME NOT NULL COMMENT '用户登录时间',
        login_ip INT unsigned NOT NULL COMMENT '登录IP',
        login_type TINYINT NOT NULL COMMENT '登录类型:0未成功 1成功'
      ) ENGINE=InnoDB ;`
	s.mustRunExec(c, sql)
	sql = `ALTER TABLE customer_login_log
		  exchange PARTITION p1 WITH TABLE arch_customer_login_log;`
	s.mustRunExec(c, sql)

	s.mustRunExec(c, `drop table if exists t1;`)
	sql = `create table t1(id int primary key,c1 varchar(100),bill_date datetime);`
	s.mustRunExec(c, sql)

	sql = `alter table t1 partition by range (to_days(bill_date)) (
		partition p202201 values less than (to_days('2022-02-01')) ENGINE = InnoDB,
		partition p202202 values less than (to_days('2022-03-01')) ENGINE = InnoDB
		);`
	s.testErrorCode(c, sql)

	sql = `alter table t1 remove partitioning;`
	s.testErrorCode(c, sql,
		session.NewErrf("Partition management on a not partitioned table is not possible."))

	sql = `drop table if exists t1;CREATE TABLE t1 (
			customer_id int(10) unsigned NOT NULL COMMENT '登录用户ID',
			login_time DATETIME NOT NULL COMMENT '用户登录时间',
			login_ip int(10) unsigned NOT NULL COMMENT '登录IP',
			login_type tinyint(4) NOT NULL COMMENT '登录类型:0未成功 1成功'
		  ) ENGINE=InnoDB
		  PARTITION BY RANGE (YEAR(login_time))(
		  PARTITION p0 VALUES LESS THAN (2017),
		  PARTITION p1 VALUES LESS THAN (2018));		  `
	s.mustRunExec(c, sql)

	sql = `alter table t1 partition by range (to_days(bill_date)) (
			partition p202201 values less than (to_days('2022-02-01')) ENGINE = InnoDB,
			partition p202202 values less than (to_days('2022-03-01')) ENGINE = InnoDB
			);`
	s.testErrorCode(c, sql)

	sql = `alter table t1 remove partitioning;`
	s.testErrorCode(c, sql)

	sql = `drop table if exists t1;CREATE TABLE t1 (
		customer_id int(10) unsigned NOT NULL COMMENT '登录用户ID',
		login_time DATETIME NOT NULL COMMENT '用户登录时间',
		month_id varchar(6) NOT NULL COMMENT '月',
		day_id varchar(2) NOT NULL COMMENT '日'
	  ) ENGINE=InnoDB
	  PARTITION BY RANGE  COLUMNS(MONTH_ID,DAY_ID) (
	  PARTITION P_20241230 VALUES LESS THAN ('202412','30'),
	  PARTITION P_20241231 VALUES LESS THAN ('202412','31'));	  `
	s.mustRunExec(c, sql)

	sql = `alter table t1 add partition (partition p_20250101 values less than ('202501','01') ENGINE = InnoDB,
		partition p_20250102 values less than ('202501','02') ENGINE = InnoDB);`
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestSubSelect(c *C) {
	config.GetGlobalConfig().Inc.EnableSelectStar = true

	s.mustRunExec(c, `drop table if exists t1,t2;
	CREATE TABLE t1(id INT,NAME VARCHAR(30));
	CREATE TABLE t2(id INT ,salesid INT ,title VARCHAR(100));`)
	sql = `SELECT a.id,a.title,(SELECT b.name FROM t1 b WHERE b.id = a.salesid) AS salesname
	FROM t2 a;`
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestUpdate(c *C) {
	saved := config.GetGlobalConfig().Inc
	defer func() {
		config.GetGlobalConfig().Inc = saved
		s.realRowCount = true
	}()

	config.GetGlobalConfig().Inc.CheckInsertField = false
	config.GetGlobalConfig().IncLevel.ErWithInsertField = 0
	config.GetGlobalConfig().Inc.EnableSetEngine = true

	// 表不存在
	sql = "update t1 set c1 = 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_NOT_EXISTED_ERROR, "test_inc.t1"))

	sql = "create table t1(id int);update t1 set id = 1;"
	s.testErrorCode(c, sql)

	sql = "create table t1(id int);update t1 as tmp set t1.id = 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.id"))

	sql = "create table t1(id int);update t1 set c1 = 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "c1"))

	sql = "create table t1(id int,c1 int);update t1 set c1 = 1,c2 = 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.c2"))

	sql = `create table t1(id int primary key,c1 int);
		update t1 s1 inner join t1 s2 on s1.id=s2.id set s1.c1=s2.c1 where s1.c1=1;`
	s.testErrorCode(c, sql)

	sql = `create table t1(id int primary key,c1 int);
		create table t2(id int primary key,c1 int,c2 int);
		update t1 inner join t2 on t1.id=t2.id2  set t1.c1=t2.c1 where c11=1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t2.id2"),
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "c11"))

	sql = `create table t1(id int primary key,c1 int);
		create table t2(id int primary key,c1 int,c2 int);
		update t1,t2 t3 set t1.c1=t2.c3 where t1.id=t3.id;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t2.c3"))

	sql = `create table t1(id int primary key,c1 int);
		create table t2(id int primary key,c1 int,c2 int);
		update t1,t2 t3 set t1.c1=t2.c3 where t1.id=t3.id;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t2.c3"))

	// where
	config.GetGlobalConfig().Inc.CheckDMLWhere = true
	sql = "create table t1(id int,c1 int);update t1 set c1 = 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NO_WHERE_CONDITION))

	sql = `create table t1(id int,c1 int);
	create table t2(id int,c1 int);`
	s.mustRunExec(c, sql)

	sql = `update t1 join t2 set t1.c1 = 1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrJoinNoOnCondition),
		session.NewErr(session.ER_NO_WHERE_CONDITION))

	sql = `update t1,t2 set t1.c1 = 1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrJoinNoOnCondition),
		session.NewErr(session.ER_NO_WHERE_CONDITION))

	sql = `update t1 NATURAL join t2  set t1.c1 = 1 ;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NO_WHERE_CONDITION))

	sql = `create table t1(id int,c1 int);
		create table t2(id int,c1 int);
		update t1 join t2 using(id) set t1.c1 = 1 ;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NO_WHERE_CONDITION))

	sql = `update t1,t2 set t1.c1 = 1 where t1.id=1;`
	s.testErrorCode(c, sql)
	config.GetGlobalConfig().Inc.CheckDMLWhere = false

	// limit
	config.GetGlobalConfig().Inc.CheckDMLLimit = true
	sql = "create table t1(id int,c1 int);update t1 set c1 = 1 limit 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_LIMIT_CONDITION))
	config.GetGlobalConfig().Inc.CheckDMLLimit = false

	// order by rand()
	config.GetGlobalConfig().Inc.CheckDMLOrderBy = true
	sql = "create table t1(id int,c1 int);update t1 set c1 = 1 order by rand();"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_ORDERBY_CONDITION))
	config.GetGlobalConfig().Inc.CheckDMLOrderBy = false

	// 受影响行数
	s.realRowCount = false
	s.mustCheck(c, "drop table if exists t1,t2;create table t1(id int,c1 int);update t1 set c1 = 1;")
	s.testAffectedRows(c, 0)

	// 受影响行数: explain计算规则
	s.mustRunExec(c, `drop table if exists t1,t2;
			create table t1(id int primary key,c1 int);
			create table t2(id int primary key,c1 int);
			insert into t1(id,c1)values(1,1);
			insert into t2(id,c1)values(1,1),(2,2),(3,3);`)

	config.GetGlobalConfig().Inc.ExplainRule = "first"
	sql = `update t1 inner join t2 on 1=1 set t1.c1=t2.c1 where 1=1;`
	s.mustCheck(c, sql)
	s.testAffectedRows(c, 1)

	config.GetGlobalConfig().Inc.ExplainRule = "max"
	s.mustCheck(c, sql)
	s.testAffectedRows(c, 3)

	s.mustRunExec(c, `drop table if exists t1,t2`)

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id int primary key,c1 int);
		create table table2(id int primary key,c1 int,c2 int);
		update table1 t1,table2 t2 set t1.c1=t2.c1 where t1.id=t2.id;`
	s.testErrorCode(c, sql)

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int);
		create table table2(id2 int primary key,c2 int,c22 int);
		update table1 t1,table2 t2 set t1.c1=t2.c2 where t1.id1=t2.id2;`
	s.testErrorCode(c, sql)

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int);
		create table table2(id2 int primary key,c2 int,c22 int);
		update table1 a1,table2 a2 set a1.c1=a2.c2 where a1.id1=a2.id2 and a1.c1=a2.c2 and a1.id1 in (1,2,3);`
	s.testErrorCode(c, sql)

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int);
		create table test.table2(id2 int primary key,c2 int,c22 int);
		update table1 a1,test.table2 a2 set a1.c1=a2.c2 where a1.id1=a2.id2 and a1.c1=a2.c2 and a1.id1 in (1,2,3);`
	s.testErrorCode(c, sql)

	s.mustRunExec(c, "drop table if exists t1;create table t1(id int,c1 int);insert into t1(id) values(1);")
	sql = `update t1 set c1=1 where id =1;`
	s.testErrorCode(c, sql)
	s.testAffectedRows(c, 1)

	sql = `drop table if exists tt1,t1;
create table tt1(id int primary key,table_schema varchar(20),table_name varchar(64),version int);
create table t1 like tt1;
UPDATE tt1
INNER JOIN
  (SELECT table_schema,
          max(VERSION) AS VERSION,
          table_name
   FROM t1
   GROUP BY table_schema)t2 ON tt1.table_schema=t2.table_schema
SET tt1.VERSION=t2.VERSION
WHERE tt1.id=1;`
	if strings.Contains(s.sqlMode, "ONLY_FULL_GROUP_BY") {
		s.testErrorCode(c, sql,
			session.NewErr(session.ErrFieldNotInGroupBy, 3, "SELECT list", "table_name"))
	} else {
		s.testErrorCode(c, sql)
	}

	sql = `drop table if exists tt1,t1;
create table tt1(id int primary key,table_schema varchar(20),table_name varchar(64),version int);
create table t1 like tt1;
UPDATE tt1
INNER JOIN
  (SELECT table_schema,
          max(VERSION) AS VERSION
   FROM t1
   GROUP BY table_schema)t2 ON tt1.table_schema=t2.table_schema
SET tt1.VERSION=t2.VERSION
WHERE tt1.id=1;`
	s.testErrorCode(c, sql)

	sql = `drop table if exists tt1,t1;
create table tt1(id int primary key,table_schema varchar(20),table_name varchar(64),version int);
create table t1 like tt1;
UPDATE tt1
INNER JOIN
  (SELECT table_schema,
          max(VERSION) AS VERSION
   FROM t1)t2 ON tt1.table_schema=t2.table_schema
SET tt1.VERSION=t2.VERSION
WHERE tt1.id=1;`
	if strings.Contains(s.sqlMode, "ONLY_FULL_GROUP_BY") {
		s.testErrorCode(c, sql,
			session.NewErr(session.ErrMixOfGroupFuncAndFields, 1, "table_schema"))
	} else {
		s.testErrorCode(c, sql)
	}

	sql = `drop table if exists t1;create table t1(id int primary key,c1 int,c2 int);
		update t1 set c1=1 and c2 = 1 where id=1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrWrongAndExpr))

	sql = `drop table if exists t1;create table t1(id int primary key,c1 int,c2 int);
		update t1 set c1=1,c2 = 1 where id=1;`
	s.testErrorCode(c, sql)

	// 列隐式转换审核
	s.mustRunExec(c, `drop table if exists t1;`)

	config.GetGlobalConfig().Inc.CheckImplicitTypeConversion = true
	sql = `create table t1(id int primary key,c1 char(100));
		update t1 s1 inner join t1 s2 on s1.id=s2.id set s1.c1=s2.c1 where s1.c1=1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrImplicitTypeConversion, "s1", "c1", "char"))

	sql = `create table t1(id int primary key,c1 varchar(100));
		update t1 s1 inner join t1 s2 on s1.id=s2.id set s1.c1=s2.c1 where s1.c1=1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrImplicitTypeConversion, "s1", "c1", "varchar"))

	sql = `create table t1(id int primary key,c1 json);
		update t1 s1 inner join t1 s2 on s1.id=s2.id set s1.c1=s2.c1 where s1.c1=1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrImplicitTypeConversion, "s1", "c1", "json"))

	sql = `create table t1(id int primary key,c1 int);
		update t1 s1 inner join t1 s2 on s1.id=s2.id set s1.c1=s2.c1 where s1.c1="1";`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrImplicitTypeConversion, "s1", "c1", "int"))

	sql = `create table t1(id int primary key,c1 int);
		update t1 s1 inner join t1 s2 on s1.id=s2.id set s1.c1=s2.c1 where s1.c1="1";`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrImplicitTypeConversion, "s1", "c1", "int"))

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int);
		create table table2(id2 int primary key,c1 int,c2 int,c22 int);
		update table1 t1,table2 t2 set c1=t2.c2 where t1.id1=t2.id2;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c1"))

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int);
		create table table2(id2 int primary key,c1 int,c2 int,c22 int);
		update table1 t1,table2 t2 set t1.c1=t2.c2 where t1.id1=t2.id2 and c1=2;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c1"))

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int,c2 int);
		create table table2(id2 int primary key,c1 int,c2 int,c22 int);
		update table1 t1,table2 t2 set t1.c1=c2 where t1.id1=t2.id2 and c1=2;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c2"),
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c1"))

	// -------------------- 多表update -------------------
	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int,c2 int);
		create table table2(id2 int primary key,c1 int,c2 int,c22 int);
		update table1 t1,table2 t2 set t1.c1=c2,t2.c22=2 where t1.id1=t2.id2 and c1=2;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c2"),
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c1"))

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int,c2 int);
		create table table2(id2 int primary key,c1 int,c2 int,c22 int);
		update table1 t1,table2 t2 set t1.c1=c2,t2.c222=2 where t1.id1=t2.id2 and c1=2;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c2"),
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t2.c222"),
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c1"))

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int,c2 int);
		create table table2(id2 int primary key,c1 int,c2 int,c22 int);
		update table1 t1,table2 t2 set t1.c1=c2,c222=2 where t1.id1=t2.id2 and c1=2;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c2"),
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "c222"),
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c1"))

	sql = `drop table if exists table1;drop table if exists table2;
		create table table1(id1 int primary key,c1 int,c2 int);
		create table table2(id2 int primary key,c1 int,c2 int,c22 int);
		update table1 t1,table2 t2 set c1=2 where t1.id1=t2.id2 and t2.c1=2;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NON_UNIQ_ERROR, "c1"))

	sql = `drop table if exists table1,table2;
		create table table1(id1 int primary key,c1 int,c2 int);
		update table1 t1 join (select 1 as id2) t2 set c1=2 where t1.id1=t2.id2;`
	s.testErrorCode(c, sql)

	sql = `drop table if exists table1,table2;
		create table table1(id1 int primary key,c1 int,c2 int);
		update table1 t1 join (select 1 as id2 union all select 2) t2 set c1=2 where t1.id1=t2.id2;`
	s.testErrorCode(c, sql)

	s.realRowCount = true
	// 受影响行数: explain计算规则
	s.mustRunExec(c, `drop table if exists t1,t2;
			create table t1(id int primary key,c1 int,c2 varchar(100));
			insert into t1(id,c1,c2)values(1,1,'_aaa'),(2,2,'aaa');`)
	// 检查转义符
	sql = `update t1 set c1 = c1 +10 where c2 like '%\_a%';`
	s.mustCheck(c, sql)
	s.testAffectedRows(c, 1)

	s.realRowCount = false
	// 受影响行数: explain计算规则
	s.mustRunExec(c, `drop table if exists t1,t2;
			create table t1(id int primary key,c1 int,c2 varchar(100));
			insert into t1(id,c1,c2)values(1,1,'_aaa'),(2,2,'aaa');`)
	// 检查转义符
	sql = `update t1 set c1 = c1 +10 where c2 like '%\_a%';`
	s.mustCheck(c, sql)
	s.testAffectedRows(c, 2)
}

func (s *testSessionIncSuite) TestDelete(c *C) {
	config.GetGlobalConfig().Inc.CheckInsertField = false
	config.GetGlobalConfig().IncLevel.ErWithInsertField = 0

	// 表不存在
	sql = "delete from t1 where c1 = 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_NOT_EXISTED_ERROR, "test_inc.t1"))

	// where
	config.GetGlobalConfig().Inc.CheckDMLWhere = true
	sql = "create table t1(id int,c1 int);delete from t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NO_WHERE_CONDITION))

	config.GetGlobalConfig().Inc.CheckDMLWhere = false

	// limit
	config.GetGlobalConfig().Inc.CheckDMLLimit = true
	sql = "create table t1(id int,c1 int);delete from t1 where id = 1 limit 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_LIMIT_CONDITION))
	config.GetGlobalConfig().Inc.CheckDMLLimit = false

	// order by rand()
	config.GetGlobalConfig().Inc.CheckDMLOrderBy = true
	sql = "create table t1(id int,c1 int);delete from t1 where id = 1 order by rand();"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_WITH_ORDERBY_CONDITION))
	config.GetGlobalConfig().Inc.CheckDMLOrderBy = false

	// 表不存在
	sql = `create table t1(id int primary key,c1 int);
		create table t2(id int primary key,c1 int,c2 int);
		delete from t3 where id1 =1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_NOT_EXISTED_ERROR, "test_inc.t3"))

	sql = `create table t1(id int primary key,c1 int);
		create table t2(id int primary key,c1 int,c2 int);
		delete from t1 where id1 =1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "id1"))

	sql = `create table t1(id int primary key,c1 int);
		create table t2(id int primary key,c1 int,c2 int);
		delete t2 from t1 inner join t2 on t1.id=t2.id2 where c11=1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t2.id2"))

	// 受影响行数
	s.mustCheck(c, "create table t1(id int,c1 int);delete from t1 where id = 1;")
	s.testAffectedRows(c, 0)

	s.mustRunExec(c, "drop table if exists t1;create table t1(id int,c1 int);insert into t1(id) values(1);")
	sql = `delete from t1 where id =1;`
	s.testErrorCode(c, sql)
	s.testAffectedRows(c, 1)

	sql = `drop table if exists t1;create table t1(id int primary key,c1 int);
		delete tmp from t1 as tmp where tmp.id=1;`
	s.testErrorCode(c, sql)

	sql = `drop table if exists t1;create table t1(id int primary key,c1 int);
		delete t1 from t1 as tmp where tmp.id=1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_NOT_EXISTED_ERROR, "test_inc.t1"))

	sql = `drop table if exists t1;create table t1(id int primary key,c1 int);
		delete s1 from t1 as s1 inner join t1 as s2 on s1.c1 = s2.c1 where s1.id > s2.id;`
	s.testErrorCode(c, sql)

	s.mustRunExec(c, "drop table if exists t1;create table t1(id int primary key,c1 int);")
	sql = `create table t2(id int primary key,c1 int);
			delete t1 from t1 inner join t2 where t2.c1 = 1;`
	s.testErrorCode(c, sql)

	// 列隐式转换审核
	config.GetGlobalConfig().Inc.CheckImplicitTypeConversion = true
	sql = `drop table if exists t1;
		create table t1(id int primary key,c1 char(100));
		delete from t1 where c1 =1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrImplicitTypeConversion, "t1", "c1", "char"))

	s.mustRunExec(c, `drop table if exists t1;CREATE TABLE t1 (
			id bigint(20) AUTO_INCREMENT primary key,
			goods_id bigint(20) unsigned NOT NULL DEFAULT '0' ,
			sku_id bigint(20) unsigned NOT NULL DEFAULT '0'  ,
			bar_code varchar(30) NOT NULL DEFAULT ''
		  );`)
	sql = "delete from t1 where id in (select id from (select any_value(id) as id,count(*) as num from t1 group by `goods_id`, `sku_id`, `bar_code` having num > 1) as t)"
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestCreateDataBase(c *C) {
	config.GetGlobalConfig().Inc.EnableDropDatabase = false
	// 不存在
	sql = "drop database if exists test1111111111111111111;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_DROP_DATABASE, "test1111111111111111111"))

	sql = "drop database test1111111111111111111;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_DROP_DATABASE, "test1111111111111111111"))
	config.GetGlobalConfig().Inc.EnableDropDatabase = true

	sql = "drop database if exists test1111111111111111111;create database test1111111111111111111;"
	s.testErrorCode(c, sql)

	// 存在
	sql = "create database test1111111111111111111;create database test1111111111111111111;"
	s.testErrorCode(c, sql,
		session.NewErrf("数据库'test1111111111111111111'已存在."))

	// if not exists 创建
	sql = "create database if not exists test1111111111111111111;create database if not exists test1111111111111111111;"
	s.testErrorCode(c, sql)

	// create database
	sql := "create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TOO_LONG_IDENT, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))

	sql = "create database mysql"
	s.testErrorCode(c, sql,
		session.NewErrf("数据库'%s'已存在.", "mysql"))

	// 字符集
	config.GetGlobalConfig().Inc.EnableSetCharset = false
	config.GetGlobalConfig().Inc.SupportCharset = ""
	sql = "drop database test1;create database test1 character set utf8;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_SET_CHARSET, "utf8"))

	config.GetGlobalConfig().Inc.SupportCharset = "utf8mb4"
	sql = "drop database test1;create database test1 character set utf8;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_SET_CHARSET, "utf8"))

	config.GetGlobalConfig().Inc.EnableSetCharset = true
	config.GetGlobalConfig().Inc.SupportCharset = "utf8,utf8mb4"
	sql = "drop database test1;create database test1 character set utf8;"
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.EnableSetCharset = true
	config.GetGlobalConfig().Inc.SupportCharset = "utf8,utf8mb4"
	sql = "drop database test1;create database test1 character set latin1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"))

	sql = `drop database test1;create database test1;
	use test1;
	create table t1(id int primary key);
	insert into t1 values(1);
	insert into t1 select count(1)+1 from t1;
	alter table t1 add column c1 int;
	update t1 set c1=1 where id=1;
	delete from t1 where id =1;`
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestAlterDataBase(c *C) {
	config.GetGlobalConfig().Inc.EnableAlterDatabase = false
	config.GetGlobalConfig().Inc.SupportCharset = "utf8mb4"
	// 不存在
	sql = "alter database test_inc12345 default character set utf8;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_DB_NOT_EXISTED_ERROR, "test_inc12345"))

	sql = "alter database test_inc default character set utf8;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NOT_SUPPORTED_YET))

	config.GetGlobalConfig().Inc.EnableAlterDatabase = true

	sql = "alter database test_inc default character set utf8;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8mb4"))

	config.GetGlobalConfig().Inc.SupportCharset = "utf8,utf8mb4"
	sql = "alter database test_inc default character set utf8;"
	s.testErrorCode(c, sql)

}

func (s *testSessionIncSuite) TestTimestampColumn(c *C) {
	sql := ""

	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '0');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '2100-1-1 1:1:1');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '1900-1-1 1:1:1');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
	sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '0');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '2100-1-1 1:1:1');`
	s.testErrorCode(c, sql)
	sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '1900-1-1 1:1:1');`
	s.testErrorCode(c, sql)

	sql = `drop table if exists timeTable;create table timeTable(c1 date default '');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
	sql = `drop table if exists timeTable;create table timeTable(c1 date default '0');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = `drop table if exists timeTable;create table timeTable(c1 date default '2100-1-1 1:1:1');`
	s.testErrorCode(c, sql)
	sql = `drop table if exists timeTable;create table timeTable(c1 date default '1900-1-1 1:1:1');`
	s.testErrorCode(c, sql)

	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp(6) default '1900-1-1 1:1:1');`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp(7) default '2000-1-1 1:1:1');`
	s.testErrorCode(c, sql,
		session.NewErrf("Too-big precision 7 specified for 'c1'. Maximum is 6."),
		session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp(0) default '2000-1-1 1:1:1');`
	s.testErrorCode(c, sql)
	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp(3) default '2000-1-1 1:1:1');`
	s.testErrorCode(c, sql)
	sql = `drop table if exists timeTable;create table timeTable(c1 timestamp(6) default '2000-1-1 1:1:1');`
	s.testErrorCode(c, sql)

	// 零值审核
	if strings.Contains(s.sqlMode, "TRADITIONAL") ||
		(strings.Contains(s.sqlMode, "STRICT_") && strings.Contains(s.sqlMode, "NO_ZERO_DATE")) {
		sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '0000-0-0');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
		sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '0000-0-0 00:00:00');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

		sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '0000-0-0');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
		sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '0000-0-0 00:00:00');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

		sql = `drop table if exists timeTable;create table timeTable(c1 date default '0000-0-0');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
		sql = `drop table if exists timeTable;create table timeTable(c1 date default '0000-0-0 00:00:00');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
	} else {
		sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '0000-0-0');`
		s.testErrorCode(c, sql)
		sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '0000-0-0 00:00:00');`
		s.testErrorCode(c, sql)

		sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '0000-0-0');`
		s.testErrorCode(c, sql)
		sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '0000-0-0 00:00:00');`
		s.testErrorCode(c, sql)

		sql = `drop table if exists timeTable;create table timeTable(c1 date default '0000-0-0');`
		s.testErrorCode(c, sql)
		sql = `drop table if exists timeTable;create table timeTable(c1 date default '0000-0-0 00:00:00');`
		s.testErrorCode(c, sql)
	}

	// 月,日零值审核
	if strings.Contains(s.sqlMode, "TRADITIONAL") ||
		(strings.Contains(s.sqlMode, "STRICT_") && strings.Contains(s.sqlMode, "NO_ZERO_IN_DATE")) {
		sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '2000-1-0 1:1:1');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

		sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '2000-1-0 1:1:1');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

		sql = `drop table if exists timeTable;create table timeTable(c1 date default '2000-1-0 1:1:1');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))
	} else {
		sql = `drop table if exists timeTable;create table timeTable(c1 timestamp default '2000-1-0 1:1:1');`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_INVALID_DEFAULT, "c1"))

		sql = `drop table if exists timeTable;create table timeTable(c1 datetime default '2000-1-0 1:1:1');`
		s.testErrorCode(c, sql)

		sql = `drop table if exists timeTable;create table timeTable(c1 date default '2000-1-0 1:1:1');`
		s.testErrorCode(c, sql)
	}
}

func (s *testSessionIncSuite) TestRenameTable(c *C) {

	// 不存在
	sql = "drop table if exists t1;create table t1(id int primary key);alter table t1 rename t2;"
	s.testErrorCode(c, sql)

	sql = "drop table if exists t1;create table t1(id int primary key);rename table t1 to t2;"
	s.testErrorCode(c, sql)

	// 存在
	sql = "drop table if exists t1;create table t1(id int primary key);rename table t1 to t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_EXISTS_ERROR, "t1"))
}

func (s *testSessionIncSuite) TestCreateView(c *C) {

	s.mustRunExec(c, "drop table if exists t1;drop view if exists v_1;")
	sql = "create table t1(id int primary key);create view v_1 as select * from t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrViewSupport, "v_1"))

	config.GetGlobalConfig().Inc.EnableUseView = true
	sql = "create table t1(id int primary key);create view v_1 as select * from t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_SELECT_ONLY_STAR))

	sql = "create table t1(id int primary key);create view v_1 as select id from t1;"
	s.testErrorCode(c, sql)

	sql = "create table t1(id int primary key,c1 int);create view v_1(id,id) as select id,c1 from t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_FIELD_SPECIFIED_TWICE, "id", "v_1"))

	sql = "create table t1(id int primary key,c1 int);create view v_1(id,c1,c2) as select id,c1 from t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrViewColumnCount))

	sql = "create table t1(id int primary key,c1 int);create view v_1(id,c1,c2) as select * from t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_SELECT_ONLY_STAR),
		session.NewErr(session.ErrViewColumnCount))

	sql = "create table t1(id int primary key,c1 int);create view v_1 as select id,c1 from t1;"
	s.testErrorCode(c, sql)

	sql = `create table t1(id int primary key,c1 int);
	create view v_1 as select id,c1 from t1 where id = 1 union all select id,c1 from t1 where id = 2;`
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestAlterTableAddIndex(c *C) {
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false

	// add index
	sql = "create table t1(id int);alter table t1 add index idx (c1)"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.c1"))

	sql = "create table t1(id int,c1 int);alter table t1 add index idx (c1);"
	s.testErrorCode(c, sql)

	sql = "create table t1(id int,c1 int);alter table t1 add index idx (c1);alter table t1 add index idx (c1);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_DUP_INDEX, "idx", "test_inc", "t1"))

	if s.DBVersion >= 50701 {
		sql = "create table t1(id int,c1 int);alter table t1 add index idx (c1);alter table t1 rename index idx to idx2;"
		s.testErrorCode(c, sql)

		sql = `create table t1(id int,c1 int);
		alter table t1 add index idx (c1),add index idx2 (c1);
		alter table t1 rename index idx to idx2;`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_DUP_KEYNAME, "idx2"))

		sql = `create table t1(id int,c1 int);
		alter table t1 add index idx (c1),add index idx2 (c1);
		alter table t1 rename index idx3 to idx2;`
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_CANT_DROP_FIELD_OR_KEY, "t1.idx3"))
	}

	sql = "create table t1(id int,c1 int,key idx (c1));alter table t1 add index idx (c1);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_DUP_INDEX, "idx", "test_inc", "t1"))

	sql = "create table t1(id int primary key);alter table t1 drop primary key;"
	s.testErrorCode(c, sql)

	sql = "CREATE TABLE geom (g GEOMETRY NOT NULL, SPATIAL INDEX ix_1(g));"
	s.testErrorCode(c, sql)

	sql = "CREATE TABLE geom (g GEOMETRY NULL, SPATIAL INDEX ix_1(g));"
	s.testErrorCode(c, sql,
		&session.SQLError{Code: 0,
			Message: "All parts of a SPATIAL index must be NOT NULL."})

	sql = "CREATE TABLE geom (id int,g GEOMETRY NOT NULL, SPATIAL INDEX ix_1(id,g));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TOO_MANY_KEY_PARTS, "ix_1", "geom", 1))

	sql = "CREATE TABLE geom (id int,g GEOMETRY NOT NULL);alter table geom add SPATIAL INDEX ix_1(id,g);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TOO_MANY_KEY_PARTS, "ix_1", "geom", 1))

	if s.DBVersion < 80000 {
		sql = `create table t1(id int,c1 int);
				alter table t1 add index idx (c1) visible;`
		s.testErrorCode(c, sql,
			session.NewErr(session.ErrUseIndexVisibility))

		sql = `create table t1(id int,c1 int);
				alter table t1 add index idx2 (c1) invisible;`
		s.testErrorCode(c, sql,
			session.NewErr(session.ErrUseIndexVisibility))

		sql = `create table t1(id int,c1 int,key idx(c1));
				alter table t1 alter index idx visible;`
		s.testErrorCode(c, sql,
			session.NewErr(session.ErrUseIndexVisibility))
	} else {
		sql = `create table t1(id int,c1 int);
		alter table t1 add index idx (c1) visible;`
		s.testErrorCode(c, sql)

		sql = `create table t1(id int,c1 int);
		alter table t1 add index idx2 (c1) invisible;`
		s.testErrorCode(c, sql)

		sql = `create table t1(id int,c1 int,key idx(c1));
				alter table t1 alter index idx visible;`
		s.testErrorCode(c, sql)
	}
}

func (s *testSessionIncSuite) TestAlterTableDropIndex(c *C) {
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false

	// drop index
	sql = "create table t1(id int);alter table t1 drop index idx"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_DROP_FIELD_OR_KEY, "t1.idx"))

	sql = "create table t1(c1 int);alter table t1 add index idx (c1);alter table t1 drop index idx;"
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestAlterTable(c *C) {
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false
	config.GetGlobalConfig().Inc.EnableDropTable = true

	// 删除后添加列
	sql = "drop table if exists t1;create table t1(id int,c1 int);alter table t1 drop column c1;alter table t1 add column c1 varchar(20);"
	s.testErrorCode(c, sql)

	sql = "drop table if exists t1;create table t1(id int,c1 int);alter table t1 drop column c1,add column c1 varchar(20);"
	s.testErrorCode(c, sql)

	// 删除后添加索引
	sql = "drop table if exists t1;create table t1(id int,c1 int,key ix(c1));alter table t1 drop index ix;alter table t1 add index ix(c1);"
	s.testErrorCode(c, sql)

	sql = "drop table if exists t1;create table t1(id int,c1 int,key ix(c1));alter table t1 drop index ix,add index ix(c1);"
	s.testErrorCode(c, sql)

	s.mustRunExec(c, "drop table if exists t1;create table t1(id int auto_increment primary key,c1 int);")
	sql = "alter table t1 auto_increment 20 comment '123';"
	s.testErrorCode(c, sql)

	if s.DBVersion < 80000 {
		config.GetGlobalConfig().Inc.MaxDDLAffectRows = 1
		s.mustRunExec(c, "insert into t1(id,c1)values(1,1),(2,2);")
		sql = "alter table t1 add column c2 int;"
		s.testErrorCode(c, sql,
			session.NewErr(session.ER_CHANGE_TOO_MUCH_ROWS, "Alter", 2, 1))
	}

	sql = `drop table if exists t1;
	create table t1(id int primary key);
	alter table t1 add column c1 geometry;
	alter table t1 add column c2 point;
	alter table t1 add column c3 linestring;
	alter table t1 add column c4 polygon;

	alter table t1 drop column c1;
	alter table t1 drop column c2;
	alter table t1 drop column c3;
	alter table t1 drop column c4; `
	s.testErrorCode(c, sql)

	// 列字符集&排序规则
	config.GetGlobalConfig().Inc.EnableColumnCharset = false
	sql = `drop table if exists t1;
    create table t1(id int primary key);
    alter table t1 add column c4 varchar(22) charset utf8;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c4"))

	sql = `drop table if exists t1;
    create table t1(id int primary key);
    alter table t1 add column c4 varchar(22) collate utf8_bin;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c4"))

	sql = `drop table if exists t1;
    create table t1(id int primary key);
    alter table t1 add column c4 varchar(22) charset utf8 collate utf8_bin;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c4"),
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c4"))

	config.GetGlobalConfig().Inc.EnableColumnCharset = true
	config.GetGlobalConfig().Inc.SupportCharset = "utf8mb4"
	sql = `drop table if exists t1;
    create table t1(id int primary key);
    alter table t1 add column c4 varchar(22) charset utf8 collate utf8_bin;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8mb4"))

	config.GetGlobalConfig().Inc.SupportCharset = "utf8"
	config.GetGlobalConfig().Inc.SupportCollation = "utf8_bin"
	sql = `drop table if exists t1;
    create table t1(id int primary key);
    alter table t1 add column c4 varchar(22) charset utf8 collate utf8mb4_bin;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCollationNotSupport, "utf8_bin"))

	config.GetGlobalConfig().Inc.EnableSetCharset = true
	config.GetGlobalConfig().Inc.EnableSetCollation = true
	config.GetGlobalConfig().Inc.SupportCharset = "utf8mb4"
	config.GetGlobalConfig().Inc.SupportCollation = "utf8_general_ci,utf8mb4_general_ci,utf8mb4_0900_ai_ci,utf8mb4_zh_0900_as_cs"
	sql = `drop table if exists t1;
		create table t1(id int primary key);
		alter table t1 DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_zh_0900_as_cs;`
	if s.DBVersion >= 80000 {
		s.testErrorCode(c, sql)
	} else {
		s.testErrorCode(c, sql,
			session.NewErrf("Collation utf8mb4_zh_0900_as_cs is only supported after mysql 8.0."))
	}

}

func (s *testSessionIncSuite) TestCreateTablePrimaryKey(c *C) {
	sql := ""

	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false

	// EnablePKColumnsOnlyInt
	config.GetGlobalConfig().Inc.EnablePKColumnsOnlyInt = true

	sql = "create table t1(id tinyint, primary key(id));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PK_COLS_NOT_INT, "id", "test_inc", "t1"))

	sql = "create table t1(id mediumint, primary key(id));"
	s.testErrorCode(c, sql)

	sql = "create table t1(id int, primary key(id));"
	s.testErrorCode(c, sql)

	sql = "create table t1(id bigint, primary key(id));"
	s.testErrorCode(c, sql)

	sql = "create table t1(id varchar(10), primary key(id));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PK_COLS_NOT_INT, "id", "test_inc", "t1"))

	sql = "create table t1(id tinyint primary key);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PK_COLS_NOT_INT, "id", "test_inc", "t1"))

	sql = "create table t1(id mediumint primary key);"
	s.testErrorCode(c, sql)

	sql = "create table t1(id int primary key);"
	s.testErrorCode(c, sql)

	sql = "create table t1(id bigint primary key);"
	s.testErrorCode(c, sql)

	sql = "create table t1(id varchar(10) primary key);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PK_COLS_NOT_INT, "id", "test_inc", "t1"))
}

func (s *testSessionIncSuite) TestTableCharsetCollation(c *C) {
	sql := ""

	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false

	// 表存在
	sql = ("create table t1(id int);create table t1(id int);")
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_EXISTS_ERROR, "t1"))

	// 字符集
	sql = `create table t1(id int,c1 varchar(20) character set utf8,
		c2 varchar(20) COLLATE utf8_bin);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c1"),
		session.NewErr(session.ER_CHARSET_ON_COLUMN, "t1", "c2"))

	config.GetGlobalConfig().Inc.EnableSetCharset = false
	sql = `create table t1(id int,c1 varchar(20)) character set utf8;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_CHARSET_MUST_NULL, "t1"))

	config.GetGlobalConfig().Inc.EnableSetCollation = false
	sql = `create table t1(id int,c1 varchar(20)) COLLATE utf8_bin;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrTableCollationNotSupport, "t1"))

	sql = `create table t1(id int,c1 varchar(20)) character set utf8 COLLATE utf8_bin;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_CHARSET_MUST_NULL, "t1"),
		session.NewErr(session.ErrTableCollationNotSupport, "t1"))

	config.GetGlobalConfig().Inc.EnableSetCharset = true
	config.GetGlobalConfig().Inc.EnableSetCollation = true
	config.GetGlobalConfig().Inc.SupportCharset = ""
	config.GetGlobalConfig().Inc.SupportCollation = ""

	sql = `create table t1(id int,c1 varchar(20)) character set utf8 COLLATE utf8_bin;`
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.SupportCharset = "utf8,utf8mb4"
	config.GetGlobalConfig().Inc.SupportCollation = "utf8_general_ci,utf8mb4_general_ci,utf8mb4_0900_ai_ci,utf8mb4_zh_0900_as_cs"
	sql = `create table t1(id int,c1 varchar(20)) DEFAULT CHARSET = utf8mb4 COLLATE utf8mb4_zh_0900_as_cs;`
	if s.DBVersion >= 80000 {
		s.testErrorCode(c, sql)
	} else {
		s.testErrorCode(c, sql,
			session.NewErrf("Collation utf8mb4_zh_0900_as_cs is only supported after mysql 8.0."))
	}

	config.GetGlobalConfig().Inc.SupportCharset = "utf8"
	config.GetGlobalConfig().Inc.SupportCollation = ""
	sql = `create table t1(id int,c1 varchar(20)) character set utf8mb4 COLLATE utf8_bin;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8"),
		session.NewErrf("COLLATION 'utf8_bin' is not valid for CHARACTER SET 'utf8mb4'!"))

	config.GetGlobalConfig().Inc.SupportCollation = "utf8_bin"
	sql = `create table t1(id int,c1 varchar(20)) character set utf8mb4 COLLATE utf8mb4_bin;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrCharsetNotSupport, "utf8"),
		session.NewErr(session.ErrCollationNotSupport, "utf8_bin"))

	config.GetGlobalConfig().Inc.SupportCharset = "utf8,utf8mb4"
	config.GetGlobalConfig().Inc.SupportCollation = "utf8_bin,utf8mb4_bin"
	if s.DBVersion >= 50700 {
		sql = `create table t1(id int primary key,c1 varchar(20),white_list VARCHAR (18000)) character set utf8mb4 COLLATE utf8mb4_bin;`
		s.testErrorCode(c, sql,
			session.NewErrf("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead.", "white_list", 16383),
			session.NewErrf("Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs."))
	} else {
		sql = `create table t1(id int primary key,c1 varchar(20),white_list VARCHAR (30000)) character set utf8 COLLATE utf8_bin;`
		s.testErrorCode(c, sql,
			session.NewErrf("Column length too big for column '%s' (max = %d); use BLOB or TEXT instead.", "white_list", 16383),
			session.NewErrf("Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs."))
	}
}

func (s *testSessionIncSuite) TestForeignKey(c *C) {
	sql := ""

	config.GetGlobalConfig().Inc.EnableForeignKey = false

	s.mustRunExec(c, "drop table if exists t2; create table t2(id int primary key,c1 int,index ix_1(c1));drop table if exists t1; ")

	sql = `create table t1(id int primary key,pid int,constraint FK_1 foreign key (pid) references t2(id));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_FOREIGN_KEY, "t1"))

	config.GetGlobalConfig().Inc.EnableForeignKey = true

	sql = `create table t1(id int primary key,pid int,constraint FK_1 foreign key (pid) references t2(id));`
	s.testErrorCode(c, sql)

	sql = `create table t1(id int primary key,pid int,constraint FK_1 foreign key (pid) references t2(id1));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t2.id1"))

	sql = `create table t1(id int primary key,pid int,constraint FK_1 foreign key (pid1) references t2(id));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_COLUMN_NOT_EXISTED, "t1.pid1"))

	sql = `create table t1(id int primary key,c1 int,c2 int,
				constraint foreign key (c1,c2) references t2(id));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrWrongFkDefWithMatch, ""))

	sql = `create table t1(id int primary key,c1 int,c2 int,
				constraint fk_1 foreign key (c1) references t2(id));`
	s.testErrorCode(c, sql)

	s.mustRunExec(c, sql)

	sql = `alter table t1 drop foreign key fk_1;`
	s.testErrorCode(c, sql)

}
func (s *testSessionIncSuite) TestZeroDate(c *C) {
	sql := ""

	config.GetGlobalConfig().Inc.EnableZeroDate = false
	sql = `create table t4 (id int unsigned not null auto_increment primary key
		comment 'primary key', a datetime not null default 0 comment 'a') comment 'test';`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DEFAULT, "a"))
	config.GetGlobalConfig().Inc.EnableZeroDate = true
}

func (s *testSessionIncSuite) TestTimestampType(c *C) {
	sql := ""

	config.GetGlobalConfig().Inc.EnableTimeStampType = false
	// sql = `create table t4 (id int unsigned not null auto_increment primary key comment 'primary key', a timestamp not null default 0 comment 'a') comment 'test';`
	sql = `create table t4 (id int unsigned not null auto_increment primary key comment 'primary key', a timestamp not null comment 'a') comment 'test';`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "a", "timestamp"))
	config.GetGlobalConfig().Inc.EnableTimeStampType = true
}

func (s *testSessionIncSuite) TestAlterNoOption(c *C) {
	sql := `drop table if exists t1;create table t1(id int,c1 int,key ix(c1));alter table t1;`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_NOT_SUPPORTED_YET))
}

func (s *testSessionIncSuite) TestFloatDouble(c *C) {
	config.GetGlobalConfig().Inc.CheckFloatDouble = true
	sql := `drop table if exists t1;create table t1(id int,c1 float,key ix(c1));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrFloatDoubleToDecimal, "c1"))

	sql = `drop table if exists t1;create table t1(id int, c2 double,key ix(c2));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrFloatDoubleToDecimal, "c2"))
	config.GetGlobalConfig().Inc.CheckFloatDouble = false
}

func (s *testSessionIncSuite) TestIdentifierUpper(c *C) {
	config.GetGlobalConfig().Inc.CheckIdentifierUpper = true
	sql := `drop table if exists hello;create table HELLO(ID int,C1 float, C2 double,key IDX_C1(C1),UNIQUE INDEX uniq_A(C2));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrIdentifierUpper, "uniq_A"),
	)

	config.GetGlobalConfig().Inc.CheckIdentifierUpper = false
}

func (s *testSessionIncSuite) TestIdentifierLower(c *C) {
	config.GetGlobalConfig().Inc.CheckIdentifierLower = true
	sql := `drop table if exists hello;create table hello(id int,c1 float, c2 double,key idx_c1(c1),unique index uniq_A(c2));`
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrIdentifierLower, "uniq_A"),
	)

	sql = `drop table if exists hello;create table hello(id int,c1 float, c2 double,key idx_c1(c1),unique index uniq_a(c2));`
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().Inc.CheckIdentifierLower = false
}

func (s *testSessionIncSuite) TestMaxKeys(c *C) {
	//er_too_many_keys
	config.GetGlobalConfig().Inc.MaxKeys = 2
	sql = "drop table if exists t1; create table t1(id int primary key,name varchar(10),age varchar(10));alter table t1 add index idx_test(id),add index idx_test2(name);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TOO_MANY_KEYS, "t1", 2))

	config.GetGlobalConfig().Inc.MaxKeys = 3
	sql = "drop table if exists t1; create table t1(id int primary key,name varchar(10),age varchar(10));alter table t1 add index idx_test(id),add index idx_test2(name);"
	s.testErrorCode(c, sql)

	//er_too_many_key_parts
	config.GetGlobalConfig().Inc.MaxKeyParts = 2
	sql = "drop table if exists t1; create table t1(id int primary key,name varchar(10),age varchar(10));alter table t1 add index idx_test(id,name,age);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TOO_MANY_KEY_PARTS, "idx_test", "t1", 2))

	config.GetGlobalConfig().Inc.MaxKeyParts = 3
	sql = "drop table if exists t1; create table t1(id int primary key,name varchar(10),age varchar(10));alter table t1 add index idx_test(id,name,age);"
	s.testErrorCode(c, sql)

	//er_pk_too_many_parts
	config.GetGlobalConfig().Inc.MaxPrimaryKeyParts = 2
	sql = "drop table if exists t1; create table t1(id int,name varchar(10),age varchar(10),primary key(id,name,age));"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PK_TOO_MANY_PARTS, "test_inc", "t1", 2))

	config.GetGlobalConfig().Inc.MaxPrimaryKeyParts = 3
	sql = "drop table if exists t1; create table t1(id int,name varchar(10),age varchar(10),primary key(id,name));"
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestSetStmt(c *C) {

	sql = `set names abc;
		set names '';
		set names utf8;
		set names utf8mb4;
		set autocommit = 1;
		`
	s.runCheck(sql)
	s.assertAudit(c, s.getResultRows(),
		[]*SQLError{
			session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"),
		},
		[]*SQLError{
			session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"),
		},
		nil,
		nil,
		[]*SQLError{
			session.NewErr(session.ER_NOT_SUPPORTED_YET),
		})

	// s.add(`set names abc`, session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"))
	// s.add(`set names ''`, session.NewErr(session.ErrCharsetNotSupport, "utf8,utf8mb4"))
	// s.add(`set names utf8`)
	// s.add(`set names utf8mb4`)
	// s.add(`set autocommit = 1`, session.NewErr(session.ER_NOT_SUPPORTED_YET))
	// s.runAudit(c)
}

func (s *testSessionIncSuite) TestMergeAlterTable(c *C) {
	//er_alter_table_once
	config.GetGlobalConfig().Inc.MergeAlterTable = true
	sql = `drop table if exists t1;
	create table t1(id int primary key,name varchar(10));
	alter table t1 add age varchar(10);
	alter table t1 add sex varchar(10);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_ALTER_TABLE_ONCE, "t1"))

	//er_alter_table_once
	config.GetGlobalConfig().Inc.MergeAlterTable = true
	sql = "drop table if exists t1; create table t1(id int primary key,name varchar(10));alter table t1 modify name varchar(10);alter table t1 modify name varchar(10);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_ALTER_TABLE_ONCE, "t1"),
	)

	//er_alter_table_once
	config.GetGlobalConfig().Inc.MergeAlterTable = true
	sql = "drop table if exists t1; create table t1(id int primary key,name varchar(10));alter table t1 change name name varchar(10);alter table t1 change name name varchar(10);"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_ALTER_TABLE_ONCE, "t1"))

}

func (s *testSessionIncSuite) TestNewRewrite(c *C) {
	var (
		newSql string
		rw     *session.Rewrite
	)

	sqls := []struct {
		sql       string
		selectSql string
		countSql  string
	}{
		{
			"insert into t2 select * from t1 where id >0;",
			"select * from t1 where id > 0",
			"select count(*) from t1 where id > 0",
		},
		{
			"insert into t2 select * from t1 where id >0 limit 10;",
			"select * from t1 where id > 0 limit 10",
			"SELECT COUNT(1) FROM (select * from t1 where id > 0 limit 10)t",
		},
		{
			"insert into t2 select * from t1 where id >0 order by c1 desc limit 10;",
			"select * from t1 where id > 0 order by c1 desc limit 10",
			"SELECT COUNT(1) FROM (select * from t1 where id > 0 limit 10)t",
		},
		{
			"insert into t2 select distinct id from t1 where id >0 order by c1 desc limit 10;",
			"select distinct id from t1 where id > 0 order by c1 desc limit 10",
			"SELECT COUNT(1) FROM (select distinct id from t1 where id > 0 order by c1 desc limit 10)t",
		},
		{
			"insert into t2 select c1,count(1) as cnt from t1 where id >0 group by c1 limit 10;",
			"select c1, count(1) as cnt from t1 where id > 0 group by c1 limit 10",
			"SELECT COUNT(1) FROM (select c1, count(1) as cnt from t1 where id > 0 group by c1 limit 10)t",
		},

		{
			"delete from t1 where id >0;",
			"select * from t1 where id > 0",
			"select count(*) from t1 where id > 0",
		},
		{
			"delete from t1 where id >0 limit 10;",
			"select * from t1 where id > 0 limit 10",
			"SELECT COUNT(1) FROM (select * from t1 where id > 0 limit 10)t",
		},
		{
			"delete from t1 where id >0 order by c1 desc limit 10;",
			"select * from t1 where id > 0 order by c1 desc limit 10",
			"SELECT COUNT(1) FROM (select * from t1 where id > 0 limit 10)t",
		},

		{
			"update t1 set c1=1 where id >0;",
			"select * from t1 where id > 0",
			"select count(*) from t1 where id > 0",
		},
		{
			"update t1 set c1=1 where id >0 limit 10;",
			"select * from t1 where id > 0 limit 10",
			"SELECT COUNT(1) FROM (select * from t1 where id > 0 limit 10)t",
		},
		{
			"update t1 set c1=1 where id >0 order by c1 desc limit 10;",
			"select * from t1 where id > 0 order by c1 desc limit 10",
			"SELECT COUNT(1) FROM (select * from t1 where id > 0 limit 10)t",
		},
		{
			"update t1 inner join t2 on t1.id=t2.id2  set t1.c1=t2.c1 where c11=1;",
			"select * from t1 join t2 on t1.id = t2.id2 where c11 = 1",
			"select count(*) from t1 join t2 on t1.id = t2.id2 where c11 = 1",
		},
		{
			"update t1,t2 set t1.c1=t2.c1 where t1.id=t2.id2 and c11=1;",
			"select * from t1, t2 where t1.id = t2.id2 and c11 = 1",
			"select count(*) from t1, t2 where t1.id = t2.id2 and c11 = 1",
		},
		{
			"update t1,t2 set t1.c1=t2.c1 where t1.id=t2.id2 and c11=1 limit 10;",
			"select * from t1, t2 where t1.id = t2.id2 and c11 = 1 limit 10",
			"SELECT COUNT(1) FROM (select * from t1, t2 where t1.id = t2.id2 and c11 = 1 limit 10)t",
		},
	}

	for _, row := range sqls {
		rw, _ = session.NewRewrite(row.sql)
		rw.RewriteDML2Select()
		c.Assert(rw.SQL, Equals, row.selectSql)

		newSql = rw.TestSelect2Count()
		c.Assert(newSql, Equals, row.countSql)
	}
}

func (s *testSessionIncSuite) TestGetAlterTablePostPart(c *C) {
	sqls := []struct {
		sql      string
		outPT    string
		outGhost string
	}{
		{
			"alter table tb_archery add unique index uniq_test_ghost_3 (test_ghost_3);",
			"ADD UNIQUE \\`uniq_test_ghost_3\\`(\\`test_ghost_3\\`)",
			"ADD UNIQUE `uniq_test_ghost_3`(`test_ghost_3`)",
		},
		{
			"alter table tb_archery add COLUMN c1 varchar(100) default null comment '!@#$%^&*(){}:<>?,./' after id123;",
			"ADD COLUMN \\`c1\\` VARCHAR(100) DEFAULT NULL COMMENT '!@#\\$%^&*(){}:<>?,./' AFTER \\`id123\\`",
			"ADD COLUMN `c1` VARCHAR(100) DEFAULT NULL COMMENT '!@#$%^&*(){}:<>?,./' AFTER `id123`",
		},
		{
			"alter table tb_archery add primary key(id);",
			"ADD PRIMARY KEY(\\`id\\`)",
			"ADD PRIMARY KEY(`id`)",
		},
		{
			"alter table tb_archery add unique key uniq_1(c1);",
			"ADD UNIQUE \\`uniq_1\\`(\\`c1\\`)",
			"ADD UNIQUE `uniq_1`(`c1`)",
		},
		{
			"alter table tb_archery alter column c1 drop default;",
			"ALTER COLUMN \\`c1\\` DROP DEFAULT",
			"ALTER COLUMN `c1` DROP DEFAULT",
		},
		{
			"alter table tb_archery default character set utf8 collate utf8_bin;",
			"CHARACTER SET UTF8 COLLATE UTF8_BIN",
			"CHARACTER SET UTF8 COLLATE UTF8_BIN",
		},
		{
			"alter table tb_archery convert to character set utf8 collate utf8_bin;",
			"CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN",
			"CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN",
		},
		{
			"alter table tb_archery collate      = utf8_bin;",
			"DEFAULT COLLATE = UTF8_BIN",
			"DEFAULT COLLATE = UTF8_BIN",
		},
		{
			"alter table t1 modify c1 varchar(100) character set utf8 collate utf8_bin;",
			"MODIFY COLUMN \\`c1\\` VARCHAR(100) CHARACTER SET UTF8 COLLATE utf8_bin",
			"MODIFY COLUMN `c1` VARCHAR(100) CHARACTER SET UTF8 COLLATE utf8_bin",
		},
		{
			"alter table t1 modify column c1 varchar(100) collate utf8_bin;",
			"MODIFY COLUMN \\`c1\\` VARCHAR(100) COLLATE utf8_bin",
			"MODIFY COLUMN `c1` VARCHAR(100) COLLATE utf8_bin",
		},
	}

	for _, row := range sqls {
		out := s.session.GetAlterTablePostPart(row.sql, true)
		c.Assert(out, Equals, row.outPT, Commentf("%v", row.sql))

		out = s.session.GetAlterTablePostPart(row.sql, false)
		c.Assert(out, Equals, row.outGhost, Commentf("%v", row.sql))
	}
}

// TestDisplayWidth 测试列指定长度参数
func (s *testSessionIncSuite) TestDisplayWidth(c *C) {
	sql := ""
	config.GetGlobalConfig().Inc.CheckColumnComment = false
	config.GetGlobalConfig().Inc.CheckTableComment = false
	config.GetGlobalConfig().Inc.EnableEnumSetBit = true

	s.mustRunExec(c, "drop table if exists t1;")
	// 数据类型 警告
	sql = `create table t1(c1 bit(100),
	c2 tinyint(1000),
	c3 smallint(1000),
	c4 mediumint(1000),
	c5 int(1000),
	c6 bigint(1000) );`
	s.testErrorCode(c, sql,
		session.NewErrf("Too big display width for column '%s' (max = 64).", "c1"),
		session.NewErrf("Too big display width for column '%s' (max = 255).", "c2"),
		session.NewErrf("Too big display width for column '%s' (max = 255).", "c3"),
		session.NewErrf("Too big display width for column '%s' (max = 255).", "c4"),
		session.NewErrf("Too big display width for column '%s' (max = 255).", "c5"),
		session.NewErrf("Too big display width for column '%s' (max = 255).", "c6"),
	)

	// 数据类型 警告
	sql = `create table t1(id int);
	alter table t1 add column c1 tinyint(1000);
	`
	s.testErrorCode(c, sql,
		session.NewErrf("Too big display width for column '%s' (max = 255).", "c1"))

}

// TestSetVariables 设置会话级变量进行审核
func (s *testSessionIncSuite) TestSetSessionVariables(c *C) {
	sql := ""

	s.mustRunExec(c, "drop table if exists t1;")

	config.GetGlobalConfig().Inc.CheckTableComment = true
	sql = `create table t1(id int primary key);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TABLE_MUST_HAVE_COMMENT, "t1"))

	sql = `inception set check_table_comment = 0;
	create table t1(id int primary key);`
	s.testErrorCode(c, sql)

	sql = `inception set check_table_comment = "123";
	create table t1(id int primary key);`

	s.testManyErrors(c, sql,
		session.NewErrf("[variable:1231]Variable 'check_table_comment' can't be set to the value of '123'."),
		session.NewErr(session.ER_TABLE_MUST_HAVE_COMMENT, "t1"))

	sql = `inception set level er_table_must_have_comment = 2;`
	s.testManyErrors(c, sql,
		session.NewErrf("暂不支持会话级的自定义审核级别."))

	sql = `inception set global check_table_comment = 1;`
	s.testManyErrors(c, sql,
		session.NewErrf("全局变量仅支持单独设置."))

	sql = `inception set lang = 'zh_cn';
		create table t1(id int primary key);
		inception set lang = 'en_us';
		create table t2(id int primary key);`
	s.testManyErrors(c, sql,
		session.NewErrf("表 't1' 需要设置注释."),
		session.NewErrf("Set comments for table 't2'."))

	config.GetGlobalConfig().Inc.CheckTableComment = false
	sql = `inception set osc_chunk_time = 1.1;;
	create table t1(id int primary key);`
	s.testErrorCode(c, sql)

}

// TestSetVariables 设置会话级变量进行审核
func (s *testSessionIncSuite) TestBlobAndText(c *C) {
	sql := ""

	s.mustRunExec(c, "drop table if exists t1,t2;")

	config.GetGlobalConfig().Inc.EnableBlobType = false
	sql = `create table t1(id int primary key,
		c1 tinyblob ,
		c2 blob,
		c3 mediumblob,
		c4 longblob);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "tinyblob"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "blob"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c3", "mediumblob"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c4", "longblob"))

	sql = `create table t2(id int primary key,
			c1 tinytext ,
			c2 text,
			c3 mediumtext,
			c4 longtext);`
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c1", "tinytext"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c2", "text"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c3", "mediumtext"),
		session.NewErr(session.ER_INVALID_DATA_TYPE, "c4", "longtext"))

}

func (s *testSessionIncSuite) TestWhereCondition(c *C) {
	sql := ""
	s.mustRunExec(c, "drop table if exists t1,t2;create table t1(id int);")

	sql = "update t1 set id = 1 where 123;"
	config.GetGlobalConfig().IncLevel.ErUseValueExpr = 0
	s.testErrorCode(c, sql)

	config.GetGlobalConfig().IncLevel.ErUseValueExpr = 1
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrUseValueExpr))

	sql = "update t1 set id = 1 where null;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ErrUseValueExpr))

	sql = `
	update t1 set id = 1 where 1+2;
	update t1 set id = 1 where 1-2;
	update t1 set id = 1 where 1*2;
	delete from t1 where 1/2;
	delete from t1 where 1&2;
	delete from t1 where 1|2;
	delete from t1 where 1^2;
	delete from t1 where 1 div 2;
	`
	s.testManyErrors(c, sql,
		session.NewErr(session.ErrUseValueExpr),
		session.NewErr(session.ErrUseValueExpr),
		session.NewErr(session.ErrUseValueExpr),
		session.NewErr(session.ErrUseValueExpr),
		session.NewErr(session.ErrUseValueExpr),
		session.NewErr(session.ErrUseValueExpr),
		session.NewErr(session.ErrUseValueExpr),
		session.NewErr(session.ErrUseValueExpr),
	)

	sql = `
	update t1 set id = 1 where 1+2=3;
	update t1 set id = 1 where id is null;
	update t1 set id = 1 where id is not null;
	update t1 set id = 1 where 1=1;
	update t1 set id = 1 where id in (1,2);
	update t1 set id = 1 where id is true;
	`
	s.testManyErrors(c, sql)
}

func (s *testSessionIncSuite) TestGroupBy(c *C) {
	sql := ""
	s.mustRunExec(c, `drop table if exists system_menu;
	create table system_menu(id int primary key,pid int ,c1 int);`)

	sql = `SELECT count(pid) as as_count_pid,pid as as_pid FROM system_menu group by as_count_pid;`
	if strings.Contains(s.sqlMode, "ONLY_FULL_GROUP_BY") {
		s.testErrorCode(c, sql,
			session.NewErrf("Can't group on 'as_count_pid'."))
	} else {
		s.testErrorCode(c, sql)
	}

	sql = `SELECT count(pid) as as_count_pid,pid as as_pid FROM system_menu group by as_pid;`
	s.testErrorCode(c, sql)

	sql = `SELECT count(pid) as as_count_pid,c1 as as_pid FROM system_menu group by c1;`
	s.testErrorCode(c, sql)

	sql = `SELECT count(pid) as as_count_pid,id as as_pid FROM system_menu group by pid;`
	if strings.Contains(s.sqlMode, "ONLY_FULL_GROUP_BY") {
		s.testErrorCode(c, sql,
			session.NewErr(session.ErrFieldNotInGroupBy, 2, "SELECT list", "id"))
	} else {
		s.testErrorCode(c, sql)
	}

	sql = `SELECT count(pid) as as_count_pid,concat(c1,'..') as as_pid FROM system_menu group by pid;`
	if strings.Contains(s.sqlMode, "ONLY_FULL_GROUP_BY") {
		s.testErrorCode(c, sql,
			session.NewErr(session.ErrFieldNotInGroupBy, 2, "SELECT list", "c1"))
	} else {
		s.testErrorCode(c, sql)
	}

}

// TestCheckAuditSetting 自动校准旧的审核规则和自定义规则, 保证两者一致
func (s *testSessionIncSuite) TestCheckAuditSetting(c *C) {
	configLevel := &config.GetGlobalConfig().IncLevel
	configLevel.ErUseEnum = 1
	configLevel.ErJsonTypeSupport = 2
	configLevel.ErUseTextOrBlob = 2

	obj := config.GetGlobalConfig().IncLevel
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)

	incLevel := make(map[string]uint8, v.NumField())

	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).CanInterface() {
			a := v.Field(i).Uint()
			if a > 2 {
				a = 2
			}
			if k := t.Field(i).Tag.Get("toml"); k != "" {
				incLevel[k] = uint8(a)
			} else {
				incLevel[t.Field(i).Name] = uint8(a)
			}
		}
	}

	for e := range session.ErrorsDefault {
		name := e.String()
		if level, ok := incLevel[name]; ok {
			v := session.GetErrorLevel(e)
			// log.Errorf("name:%v,incLevel:%d, config: %d", name, level, v)
			c.Assert(level, Equals, v, Commentf("name:%v,incLevel:%d, config: %d", name, level, v))
		}
	}
}

func (s *testSessionIncSuite) TestCreateProcedure(c *C) {
	config.GetGlobalConfig().Inc.EnableCreateProcedure = false
	sql := ""
	sql = "create procedure sp_t1() begin select 1;end;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PROCEDURE_NOT_ALLOWED))

	config.GetGlobalConfig().Inc.EnableCreateProcedure = true
	sql = "create procedure sp_t1() begin select 1;end;"
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestDropProcedure(c *C) {
	config.GetGlobalConfig().Inc.EnableDropProcedure = false
	sql := ""
	sql = "drop procedure sp_t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_DROP_PROCEDURE, "sp_t1"))

	config.GetGlobalConfig().Inc.EnableDropProcedure = true
	sql = "drop procedure sp_t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_PROCEDURE_NOT_EXISTED_ERROR, "sp_t1"))
}

func (s *testSessionIncSuite) TestCreateFunction(c *C) {
	config.GetGlobalConfig().Inc.EnableCreateFunction = false
	sql := ""
	sql = "create function sp_t1() returns int RETURN 1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_FUNCTION_NOT_ALLOWED))

	config.GetGlobalConfig().Inc.EnableCreateFunction = true
	sql = "create function sp_t1() returns int RETURN 1;"
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestDropFunction(c *C) {
	config.GetGlobalConfig().Inc.EnableDropFunction = false
	sql := ""
	sql = "drop function sp_t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_DROP_FUNCTION, "sp_t1"))

	config.GetGlobalConfig().Inc.EnableDropFunction = true
	sql = "drop function sp_t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_FUNCTION_NOT_EXISTED_ERROR, "sp_t1"))
}

func (s *testSessionIncSuite) TestCreateTrigger(c *C) {
	config.GetGlobalConfig().Inc.EnableCreateTrigger = false
	sql := ""
	sql = "create table t1(id int);create trigger t1 before insert on t1 for each row begin select 1;end;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TRIGGER_NOT_ALLOWED))

	config.GetGlobalConfig().Inc.EnableCreateTrigger = true
	sql = "create table t1(id int);create trigger t1 before insert on t1 for each row begin select 1;end;"
	s.testErrorCode(c, sql)
}

func (s *testSessionIncSuite) TestDropTrigger(c *C) {
	config.GetGlobalConfig().Inc.EnableDropTrigger = false
	sql := ""
	sql = "drop trigger t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_CANT_DROP_TRIGGER, "t1"))

	config.GetGlobalConfig().Inc.EnableDropTrigger = true
	sql = "drop trigger t1;"
	s.testErrorCode(c, sql,
		session.NewErr(session.ER_TRIGGER_NOT_EXISTED_ERROR, "t1"))
}
