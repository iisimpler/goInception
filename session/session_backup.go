package session

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/replication"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/mysql"
	"github.com/jinzhu/gorm"
	log "github.com/sirupsen/logrus"
)

const backupTableHostDataLength = 128

// chanBackup 备份channal数据,用来传递备份的sql等信息
type chanBackup struct {
	// values []interface{}
	// 库名
	dbname string
	values []interface{}
	record *Record
}

func (s *session) processChanBackup(wg *sync.WaitGroup) {
	for {
		r := <-s.chBackupRecord

		if r == nil {
			s.flushBackupRecord(s.lastBackupTable, s.myRecord)
			wg.Done()
			break
		}
		// flush标志. 不能在外面调用flush函数,会导致线程并发操作,写入数据错误
		// 如数据尚未进入到ch通道,此时调用flush,数据无法正确入库
		if r.values == nil {
			s.flushBackupRecord(r.dbname, r.record)
		} else {
			s.writeBackupRecord(r.dbname, r.record, r.values)
		}
	}
}

func (s *session) runBackup(ctx context.Context) {
	log.Debug("runBackup")
	var wg sync.WaitGroup
	wg.Add(1)
	s.chBackupRecord = make(chan *chanBackup, 50)
	go s.processChanBackup(&wg)
	defer func() {
		close(s.chBackupRecord)
		wg.Wait()
		// 清空临时的库名
		s.lastBackupTable = ""
	}()

	for _, record := range s.recordSets.All() {

		if s.checkSqlIsDML(record) || s.checkSqlIsDDL(record) {
			s.myRecord = record

			if record.TableInfo != nil {
				longDataType, hostMaxLength := s.mysqlCreateBackupTable(record)
				s.mysqlBackupSql(record, longDataType, hostMaxLength)
			} else if record.SequencesInfo != nil {
				longDataType, hostMaxLength := s.mysqlCreateBackupSequencesTable(record)
				s.mysqlBackupSql(record, longDataType, hostMaxLength)
			} else {
				if record.TableInfo == nil {
					s.appendErrorNo(ErrNotFoundTableInfo)
				} else if record.SequencesInfo == nil {
					s.appendErrorNo(ErrNotFoundTableInfo)
				}
			}

			// if record.TableInfo != nil {
			// 	log.Errorf("mysqlCreateBackupTable: %v, %v,%v", record.TableInfo.Name, longDataType, hostMaxLength)
			// } else {
			// 	log.Errorf("mysqlCreateBackupTable: record: %v, %v,%v", record, longDataType, hostMaxLength)
			// }

			if s.hasError() {
				break
			}
		}

		// // 进程Killed
		// if err := checkClose(ctx); err != nil {
		//     log.Warn("Killed: ", err)
		//     s.AppendErrorMessage("Operation has been killed!")
		//     break
		// }
	}
}

// 解析的sql写入缓存,并定期入库
func (s *session) writeBackupRecord(dbname string, record *Record, values []interface{}) {

	s.insertBuffer = append(s.insertBuffer, values...)

	// 每500行insert提交一次
	if len(s.insertBuffer) >= 500*11 {
		s.flushBackupRecord(dbname, record)
	}
}

// flush用以写入当前insert缓存,并清空缓存.
func (s *session) flushBackupRecord(dbname string, record *Record) {
	// log.Info("flush ", len(s.insertBuffer))

	if len(s.insertBuffer) > 0 {
		const backupRecordColumnCount int = 11
		const rowSQL = "(?,?,?,?,?,?,?,?,?,?,NOW(),?),"
		tableName := fmt.Sprintf("`%s`.`%s`", dbname, remoteBackupTable)

		sql := "insert into %s values%s"
		values := strings.TrimRight(
			strings.Repeat(rowSQL, len(s.insertBuffer)/backupRecordColumnCount), ",")

		err := s.backupdb.Exec(fmt.Sprintf(sql, tableName, values),
			s.insertBuffer...).Error
		if err != nil {
			log.Error(err)
			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
				s.recordSets.MaxLevel = 2
				record.StageStatus = StatusBackupFail
				record.appendErrorMessage(myErr.Message)
			}
		}

		// s.BackupTotalRows += len(s.insertBuffer) / backupRecordColumnCount
		// s.SetMyProcessInfo(record.Sql, time.Now(),
		//     float64(s.BackupTotalRows)/float64(s.TotalChangeRows))

		s.insertBuffer = nil
	}
}

func (s *session) mysqlExecuteBackupSqlForDDL(record *Record) {
	if record.DDLRollback == "" {
		return
	}

	var buf strings.Builder
	buf.WriteString("INSERT INTO ")
	dbname := s.getRemoteBackupDBName(record)
	buf.WriteString(fmt.Sprintf("`%s`.`%s`", dbname, record.TableInfo.Name))
	buf.WriteString("(rollback_statement, opid_time) VALUES('")
	buf.WriteString(HTMLEscapeString(record.DDLRollback))
	buf.WriteString("','")
	buf.WriteString(record.OPID)
	buf.WriteString("')")

	sql := buf.String()

	if err := s.backupdb.Exec(sql).Error; err != nil {
		log.Errorf("con:%d %v sql:%s", s.sessionVars.ConnectionID, err, sql)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err.Error())
		}
		record.StageStatus = StatusBackupFail
	} else {
		record.StageStatus = StatusBackupOK
	}
}

// mysqlExecuteBackupInfoInsertSql 写入备份记录表
// longDataType 为true表示字段类型已更新,否则为text,需要在写入时自动截断
func (s *session) mysqlExecuteBackupInfoInsertSql(record *Record, longDataType bool, hostMaxLength int) int {

	record.OPID = makeOPIDByTime(record.ExecTimestamp, record.ThreadId, record.SeqNo)

	typeStr := "UNKNOWN"
	switch record.Type.(type) {
	case *ast.InsertStmt:
		typeStr = "INSERT"
	case *ast.DeleteStmt:
		typeStr = "DELETE"
	case *ast.UpdateStmt:
		typeStr = "UPDATE"
	case *ast.CreateDatabaseStmt:
		typeStr = "CREATEDB"
	case *ast.CreateTableStmt:
		typeStr = "CREATETABLE"
	case *ast.CreateViewStmt:
		typeStr = "CREATEVIEW"
	case *ast.AlterTableStmt:
		typeStr = "ALTERTABLE"
	case *ast.DropTableStmt:
		typeStr = "DROPTABLE"
	case *ast.RenameTableStmt:
		typeStr = "RENAMETABLE"
	case *ast.CreateIndexStmt:
		typeStr = "CREATEINDEX"
	case *ast.DropIndexStmt:
		typeStr = "DROPINDEX"
	case *ast.CreateSequenceStmt:
		typeStr = "CREATESEQUENCES"
	case *ast.DropSequenceStmt:
		typeStr = "DROPSEQUENCES"
	case *ast.AlterSequenceStmt:
		typeStr = "ALTERSEQUENCES"
	default:
		log.Warning("类型未知: ", record.Type)
	}

	sql_stmt := HTMLEscapeString(record.Sql)

	// 已更新sql_statement类型为mediumtext
	// longDataType 为true表示字段类型已更新,否则为text,需要在写入时自动截断

	// 最大可存储65535个字节(64KB-1)
	if !longDataType && len(sql_stmt) > (1<<16)-1 {

		s.appendWarning(ErrDataTooLong, "sql_statement", 1)

		sql_stmt = sql_stmt[:(1<<16)-4]
		// 如果误截取了utf8字符,则往前找最后一个有效字符
		for {
			ch, _ := utf8.DecodeLastRuneInString(sql_stmt)
			if ch != utf8.RuneError {
				break
			}
			sql_stmt = sql_stmt[:len(sql_stmt)-1]
		}
		sql_stmt = sql_stmt + "..."
	}

	host := s.opt.Host
	if hostMaxLength > 0 {
		if len(host) > hostMaxLength {
			host = host[:hostMaxLength]
		}
	}
	var values []interface{}
	values = append(values, record.OPID)
	values = append(values, record.StartFile)
	values = append(values, strconv.Itoa(record.StartPosition))
	values = append(values, record.EndFile)
	values = append(values, strconv.Itoa(record.EndPosition))
	values = append(values, sql_stmt)
	values = append(values, host)

	if record.TableInfo != nil {
		values = append(values, record.TableInfo.Schema)
		values = append(values, record.TableInfo.Name)
	} else if record.SequencesInfo != nil {
		values = append(values, record.SequencesInfo.Schema)
		values = append(values, record.SequencesInfo.Name)
	}

	values = append(values, strconv.Itoa(s.opt.Port))
	values = append(values, typeStr)

	dbName := s.getRemoteBackupDBName(record)

	if s.lastBackupTable == "" {
		s.lastBackupTable = dbName
	}
	// 库名改变时强制flush
	if s.lastBackupTable != dbName {
		s.chBackupRecord <- &chanBackup{
			dbname: s.lastBackupTable,
			record: record,
			values: nil,
		}
		s.lastBackupTable = dbName
	}

	s.chBackupRecord <- &chanBackup{
		dbname: dbName,
		record: record,
		values: values,
	}

	return 0
}

func (s *session) mysqlCreateSqlBackupTable(dbname string) string {

	// if not exists
	buf := bytes.NewBufferString("CREATE TABLE  ")

	buf.WriteString(fmt.Sprintf("`%s`.`%s`", dbname, remoteBackupTable))
	buf.WriteString("(")

	buf.WriteString("opid_time varchar(50),")
	buf.WriteString("start_binlog_file varchar(512),")
	buf.WriteString("start_binlog_pos int,")
	buf.WriteString("end_binlog_file varchar(512),")
	buf.WriteString("end_binlog_pos int,")
	buf.WriteString("sql_statement mediumtext,")
	buf.WriteString("host VARCHAR(")
	buf.WriteString(strconv.Itoa(backupTableHostDataLength))
	buf.WriteString("),")
	buf.WriteString("dbname VARCHAR(64),")
	buf.WriteString("tablename VARCHAR(64),")
	buf.WriteString("port INT,")
	buf.WriteString("time TIMESTAMP,")
	buf.WriteString("type VARCHAR(20),")
	buf.WriteString("PRIMARY KEY(opid_time)")

	buf.WriteString(")ENGINE INNODB DEFAULT CHARSET UTF8MB4;")

	return buf.String()
}

func (s *session) mysqlCreateSqlFromTableInfo(dbname string, ti *TableInfo) string {
	buf := bytes.NewBufferString("CREATE TABLE if not exists ")
	buf.WriteString(fmt.Sprintf("`%s`.`%s`", dbname, ti.Name))
	buf.WriteString("(")

	buf.WriteString("id bigint auto_increment primary key, ")
	buf.WriteString("rollback_statement mediumtext, ")
	buf.WriteString("opid_time varchar(50),")
	buf.WriteString("KEY `idx_opid_time` (`opid_time`)")

	buf.WriteString(") ENGINE INNODB DEFAULT CHARSET UTF8MB4;")

	return buf.String()
}

func (s *session) mysqlCreateSqlFromSequencesInfo(dbname string, ti *SequencesInfo) string {
	buf := bytes.NewBufferString("CREATE TABLE if not exists ")
	buf.WriteString(fmt.Sprintf("`%s`.`%s`", dbname, ti.Name))
	buf.WriteString("(")

	buf.WriteString("id bigint auto_increment primary key, ")
	buf.WriteString("rollback_statement mediumtext, ")
	buf.WriteString("opid_time varchar(50),")
	buf.WriteString("KEY `idx_opid_time` (`opid_time`)")

	buf.WriteString(") ENGINE INNODB DEFAULT CHARSET UTF8MB4;")

	return buf.String()
}

func (s *session) getRemoteBackupDBName(record *Record) string {

	if record.BackupDBName != "" {
		return record.BackupDBName
	}
	var v string
	if record.TableInfo != nil {
		v = fmt.Sprintf("%s_%d_%s", s.opt.Host, s.opt.Port, record.TableInfo.Schema)
	} else if record.SequencesInfo != nil {
		v = fmt.Sprintf("%s_%d_%s", s.opt.Host, s.opt.Port, record.SequencesInfo.Schema)
	}

	if len(v) > mysql.MaxDatabaseNameLength {
		v = v[len(v)-mysql.MaxDatabaseNameLength:]
		// s.AppendErrorNo(ER_TOO_LONG_BAKDB_NAME, s.opt.host, s.opt.port, record.TableInfo.Schema)
		// return ""
	}

	v = strings.Replace(v, "-", "_", -1)
	v = strings.Replace(v, ".", "_", -1)
	record.BackupDBName = v
	return record.BackupDBName
}

// mysqlCreateBackupTable 创建备份表.
// 如果备份表的表结构是旧表结构,即sql_statement字段类型为text,则返回false,否则返回true
// longDataType 为true表示字段类型已更新,否则为text,需要在写入时自动截断
// hostMaxLength 新建表时默认为128,旧表则从数据库读取
func (s *session) mysqlCreateBackupTable(record *Record) (
	longDataType bool, hostMaxLength int) {

	if record.TableInfo == nil {
		return
	}

	backupDBName := s.getRemoteBackupDBName(record)
	if backupDBName == "" {
		return
	}

	if record.TableInfo.IsCreated {
		// 返回longDataType值
		key := fmt.Sprintf("%s.%s", backupDBName, remoteBackupTable)
		if cache, ok := s.backupTableCacheList[key]; ok {
			return cache.longDataType, cache.hostMaxLength
		}
		return
	}

	if _, ok := s.backupDBCacheList[backupDBName]; !ok {
		sql := fmt.Sprintf("create database if not exists `%s`;", backupDBName)
		if err := s.backupdb.Exec(sql).Error; err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
				if myErr.Number != 1007 { /*ER_DB_CREATE_EXISTS*/
					s.appendErrorMsg(myErr.Message)
					return
				}
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		}
		s.backupDBCacheList[backupDBName] = true
	}

	key := fmt.Sprintf("%s.%s", backupDBName, record.TableInfo.Name)
	if _, ok := s.backupTableCacheList[key]; !ok {
		createSql := s.mysqlCreateSqlFromTableInfo(backupDBName, record.TableInfo)
		if err := s.backupdb.Exec(createSql).Error; err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
				if myErr.Number != 1050 { /*ER_TABLE_EXISTS_ERROR*/
					s.appendErrorMsg(myErr.Message)
					return
				}
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		}
		s.backupTableCacheList[key] = BackupTable{
			longDataType:  true,
			hostMaxLength: backupTableHostDataLength,
		}
	}

	key = fmt.Sprintf("%s.%s", backupDBName, remoteBackupTable)
	if _, ok := s.backupTableCacheList[key]; !ok {
		createSql := s.mysqlCreateSqlBackupTable(backupDBName)
		if err := s.backupdb.Exec(createSql).Error; err != nil {
			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
				if myErr.Number != 1050 { /*ER_TABLE_EXISTS_ERROR*/
					log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
					s.appendErrorMsg(myErr.Message)
					return
				}
				// 获取sql_statement字段类型,用以兼容类型为text的旧表结构
				longDataType = s.checkBackupTableSqlStmtColumnType(backupDBName)
				// host字段长度
				hostMaxLength = s.checkBackupTableHostMaxLength(backupDBName)
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		} else {
			longDataType = true
			hostMaxLength = backupTableHostDataLength
		}
		s.backupTableCacheList[key] = BackupTable{
			longDataType:  longDataType,
			hostMaxLength: hostMaxLength,
		}
	}

	// 从remoteBackupTable表获取cache
	if cache, ok := s.backupTableCacheList[key]; ok {
		longDataType = cache.longDataType
		hostMaxLength = cache.hostMaxLength
	}
	record.TableInfo.IsCreated = true
	return
}

func (s *session) mysqlCreateBackupSequencesTable(record *Record) (
	longDataType bool, hostMaxLength int) {

	if record.SequencesInfo == nil {
		return
	}

	backupDBName := s.getRemoteBackupDBName(record)
	if backupDBName == "" {
		return
	}

	if record.SequencesInfo.IsCreated {
		// 返回longDataType值
		key := fmt.Sprintf("%s.%s", backupDBName, remoteBackupTable)
		if cache, ok := s.backupTableCacheList[key]; ok {
			return cache.longDataType, cache.hostMaxLength
		}
		return
	}

	if _, ok := s.backupDBCacheList[backupDBName]; !ok {
		sql := fmt.Sprintf("create database if not exists `%s`;", backupDBName)
		if err := s.backupdb.Exec(sql).Error; err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
				if myErr.Number != 1007 { /*ER_DB_CREATE_EXISTS*/
					s.appendErrorMsg(myErr.Message)
					return
				}
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		}
		s.backupDBCacheList[backupDBName] = true
	}

	key := fmt.Sprintf("%s.%s", backupDBName, record.SequencesInfo.Name)
	if _, ok := s.backupTableCacheList[key]; !ok {
		createSql := s.mysqlCreateSqlFromSequencesInfo(backupDBName, record.SequencesInfo)
		if err := s.backupdb.Exec(createSql).Error; err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
				if myErr.Number != 1050 { /*ER_TABLE_EXISTS_ERROR*/
					s.appendErrorMsg(myErr.Message)
					return
				}
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		}
		s.backupTableCacheList[key] = BackupTable{
			longDataType:  false,
			hostMaxLength: backupTableHostDataLength,
		}
	}

	key = fmt.Sprintf("%s.%s", backupDBName, remoteBackupTable)
	if _, ok := s.backupTableCacheList[key]; !ok {
		createSql := s.mysqlCreateSqlBackupTable(backupDBName)
		if err := s.backupdb.Exec(createSql).Error; err != nil {
			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
				if myErr.Number != 1050 { /*ER_TABLE_EXISTS_ERROR*/
					log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
					s.appendErrorMsg(myErr.Message)
					return
				}
				// 获取sql_statement字段类型,用以兼容类型为text的旧表结构
				//longDataType = s.checkBackupTableSqlStmtColumnType(backupDBName)
				// host字段长度
				hostMaxLength = s.checkBackupTableHostMaxLength(backupDBName)
			} else {
				s.appendErrorMsg(err.Error())
				return
			}
		} else {
			longDataType = false
			hostMaxLength = backupTableHostDataLength
		}
		s.backupTableCacheList[key] = BackupTable{
			longDataType:  longDataType,
			hostMaxLength: hostMaxLength,
		}
	}

	// 从remoteBackupTable表获取cache
	if cache, ok := s.backupTableCacheList[key]; ok {
		longDataType = cache.longDataType
		hostMaxLength = cache.hostMaxLength
	}
	record.SequencesInfo.IsCreated = true
	return
}

// checkBackupTableSqlStmtColumnType 检查sql_statement字段类型,用以兼容类型为text的旧表结构
func (s *session) checkBackupTableSqlStmtColumnType(dbname string) (longDataType bool) {

	// 获取sql_statement字段类型,用以兼容类型为text的旧表结构
	sql := fmt.Sprintf(`select DATA_TYPE from information_schema.columns
					where table_schema='%s' and table_name='%s' and column_name='sql_statement';`,
		dbname, remoteBackupTable)

	var res string

	rows, err2 := s.backupdb.DB().Query(sql)
	if err2 != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err2)
		if myErr, ok := err2.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err2.Error())
		}
	}
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			_ = rows.Scan(&res)
		}
		return res != "text"
	}

	return

}

// checkBackupTableHostMaxLength 检查host字段类型的长度
func (s *session) checkBackupTableHostMaxLength(dbname string) (length int) {

	// 获取sql_statement字段类型,用以兼容类型为text的旧表结构
	sql := fmt.Sprintf(`select COLUMN_TYPE from information_schema.columns
					where table_schema='%s' and table_name='%s' and column_name='host';`,
		dbname, remoteBackupTable)

	var res string

	rows, err2 := s.backupdb.DB().Query(sql)
	if err2 != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err2)
		if myErr, ok := err2.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMsg(myErr.Message)
		} else {
			s.appendErrorMsg(err2.Error())
		}
	}
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			_ = rows.Scan(&res)
		}
		if res != "" {
			typeLength := GetDataTypeLength(res)
			if len(typeLength) > 0 {
				return typeLength[0]
			}
		}
		return 0
	}

	return 0

}

func (s *session) needTransactionMark() bool {
	return s.dbType == DBTypeOceanBase
}

type transactionMarkType int

const (
	transactionMarkTypeStart transactionMarkType = iota
	transactionMarkTypeEnd
)

type TransactionMarkData struct {
	ThreadID    uint32
	LogFile     string
	LogPosition int
}

func (s *session) createTransactionMarkTable() {
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", transactionMarkDb)
	if s.executeInternal(sql) {
		sql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ("+
			"id INT AUTO_INCREMENT PRIMARY KEY, "+
			"mark_type INT, "+
			"thread_id INT UNSIGNED, "+
			"log_file VARCHAR(40), "+
			"log_position INT "+
			")", transactionMarkDb, transactionMarkTable)
		s.executeInternal(sql)
	}
}

func (s *session) markTransactionStart(tx *gorm.DB, data *TransactionMarkData) *gorm.DB {
	return s.insertTransactionMark(tx, transactionMarkTypeStart, data)
}

func (s *session) markTransactionEnd(tx *gorm.DB, data *TransactionMarkData) *gorm.DB {
	return s.insertTransactionMark(tx, transactionMarkTypeEnd, data)
}

func (s *session) insertTransactionMark(tx *gorm.DB, markType transactionMarkType, data *TransactionMarkData) *gorm.DB {
	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` "+
		"(mark_type, thread_id, log_file, log_position) "+
		"VALUES (%d, %d,'%s', %d)", transactionMarkDb, transactionMarkTable,
		markType, data.ThreadID, data.LogFile, data.LogPosition)
	return tx.Exec(sql)
}

func (s *session) isTransactionMark(dbname string, table string) bool {
	return strings.EqualFold(dbname, transactionMarkDb) && strings.EqualFold(table, transactionMarkTable)
}

type transactionMark struct {
	ID       int32
	MarkType transactionMarkType
	TransactionMarkData
}

func (s *session) toTransactionMark(e *replication.BinlogEvent) *transactionMark {
	if event, ok := e.Event.(*replication.RowsEvent); ok {
		if s.isTransactionMark(string(event.Table.Schema), string(event.Table.Table)) {
			for _, rows := range event.Rows {
				mark := &transactionMark{}
				mark.ID = rows[0].(int32)
				mark.MarkType = transactionMarkType(rows[1].(int32))
				mark.ThreadID = uint32(1<<32 + int64(rows[2].(int32)))
				mark.LogFile = rows[3].(string)
				mark.LogPosition = int(rows[4].(int32))
				return mark
			}
		}
	}
	return nil
}

func (s *session) executeInternal(sql string) bool {
	if _, err := s.exec(sql, true); err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMsg(myErr.Message + " (sql: " + sql + ")")
		} else {
			s.appendErrorMsg(err.Error())
		}
		return false
	}
	return true
}
