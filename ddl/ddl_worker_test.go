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

package ddl

import (
	"sync"
	"time"

	"github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/kv"
	"github.com/hanchuanchuan/goInception/meta"
	"github.com/hanchuanchuan/goInception/model"
	"github.com/hanchuanchuan/goInception/mysql"
	"github.com/hanchuanchuan/goInception/sessionctx"
	"github.com/hanchuanchuan/goInception/types"
	"github.com/hanchuanchuan/goInception/util/admin"
	"github.com/hanchuanchuan/goInception/util/mock"
	"github.com/hanchuanchuan/goInception/util/sqlexec"
	"github.com/hanchuanchuan/goInception/util/testleak"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"golang.org/x/net/context"
)

var _ = Suite(&testDDLSuite{})

type testDDLSuite struct{}

const testLease = 5 * time.Millisecond

func (s *testDDLSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	// set ReorgWaitTimeout to small value, make test to be faster.
	ReorgWaitTimeout = 50 * time.Millisecond
	WaitTimeWhenErrorOccured = 1 * time.Microsecond
}

func (s *testDDLSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testDDLSuite) TestCheckOwner(c *C) {
	store := testCreateStore(c, "test_owner")
	defer store.Close()

	d1 := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d1.Stop()
	time.Sleep(testLease)
	testCheckOwner(c, d1, true)

	c.Assert(d1.GetLease(), Equals, testLease)
}

// TestRunWorker tests no job is handled when the value of RunWorker is false.
func (s *testDDLSuite) TestRunWorker(c *C) {
	store := testCreateStore(c, "test_run_worker")
	defer store.Close()

	RunWorker = false
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	testCheckOwner(c, d, false)
	defer d.Stop()

	// Make sure the DDL worker is nil.
	worker := d.generalWorker()
	c.Assert(worker, IsNil)
	// Make sure the DDL job can be done and exit that goroutine.
	RunWorker = true
	d1 := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	testCheckOwner(c, d1, true)
	defer d1.Stop()
	worker = d1.generalWorker()
	c.Assert(worker, NotNil)
}

func (s *testDDLSuite) TestSchemaError(c *C) {
	store := testCreateStore(c, "test_schema_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	doDDLJobErr(c, 1, 0, model.ActionCreateSchema, []interface{}{1}, ctx, d)
}

func (s *testDDLSuite) TestTableError(c *C) {
	store := testCreateStore(c, "test_table_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	// Schema ID is wrong, so dropping table is failed.
	doDDLJobErr(c, -1, 1, model.ActionDropTable, nil, ctx, d)
	// Table ID is wrong, so dropping table is failed.
	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)
	job := doDDLJobErr(c, dbInfo.ID, -1, model.ActionDropTable, nil, ctx, d)

	// Table ID or schema ID is wrong, so getting table is failed.
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		job.SchemaID = -1
		job.TableID = -1
		t := meta.NewMeta(txn)
		_, err1 := getTableInfo(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		job.SchemaID = dbInfo.ID
		_, err1 = getTableInfo(t, job, job.SchemaID)
		c.Assert(err1, NotNil)
		return nil
	})
	c.Assert(err, IsNil)

	// Args is wrong, so creating table is failed.
	doDDLJobErr(c, 1, 1, model.ActionCreateTable, []interface{}{1}, ctx, d)
	// Schema ID is wrong, so creating table is failed.
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionCreateTable, []interface{}{tblInfo}, ctx, d)
	// Table exists, so creating table is failed.
	tblInfo.ID = tblInfo.ID + 1
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionCreateTable, []interface{}{tblInfo}, ctx, d)

}

func (s *testDDLSuite) TestForeignKeyError(c *C) {
	store := testCreateStore(c, "test_foreign_key_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	doDDLJobErr(c, -1, 1, model.ActionAddForeignKey, nil, ctx, d)
	doDDLJobErr(c, -1, 1, model.ActionDropForeignKey, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropForeignKey, []interface{}{model.NewCIStr("c1_foreign_key")}, ctx, d)
}

func (s *testDDLSuite) TestIndexError(c *C) {
	store := testCreateStore(c, "test_index_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	// Schema ID is wrong.
	doDDLJobErr(c, -1, 1, model.ActionAddIndex, nil, ctx, d)
	doDDLJobErr(c, -1, 1, model.ActionDropIndex, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	// for adding index
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, []interface{}{1}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("t"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "c1_index", "c1")
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), 1,
			[]*ast.IndexPartSpecification{{Column: &ast.ColumnName{Name: model.NewCIStr("c1")}, Length: 256}}}, ctx, d)

	// for dropping index
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{1}, ctx, d)
	testDropIndex(c, ctx, d, dbInfo, tblInfo, "c1_index")
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{model.NewCIStr("c1_index")}, ctx, d)
}

func (s *testDDLSuite) TestColumnError(c *C) {
	store := testCreateStore(c, "test_column_error")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)
	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)
	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c4"),
		Offset:       len(tblInfo.Columns),
		DefaultValue: 0,
	}
	col.ID = allocateColumnID(tblInfo)
	col.FieldType = *types.NewFieldType(mysql.TypeLong)
	pos := &ast.ColumnPosition{Tp: ast.ColumnPositionAfter, RelativeColumn: &ast.ColumnName{Name: model.NewCIStr("c5")}}

	// for adding column
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, -1, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, []interface{}{0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)

	// for dropping column
	doDDLJobErr(c, -1, tblInfo.ID, model.ActionDropColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, -1, model.ActionDropColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropColumn, []interface{}{0}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropColumn, []interface{}{model.NewCIStr("c5")}, ctx, d)
}

func testCheckOwner(c *C, d *ddl, expectedVal bool) {
	c.Assert(d.isOwner(), Equals, expectedVal)
}

func testCheckJobDone(c *C, d *ddl, job *model.Job, isAdd bool) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		checkHistoryJob(c, historyJob)
		if isAdd {
			c.Assert(historyJob.SchemaState, Equals, model.StatePublic)
		} else {
			c.Assert(historyJob.SchemaState, Equals, model.StateNone)
		}

		return nil
	})
}

func testCheckJobCancelled(c *C, d *ddl, job *model.Job, state *model.SchemaState) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.IsCancelled() || historyJob.IsRollbackDone(), IsTrue, Commentf("histroy job %s", historyJob))
		if state != nil {
			c.Assert(historyJob.SchemaState, Equals, *state)
		}
		return nil
	})
}

func doDDLJobErrWithSchemaState(ctx sessionctx.Context, d *ddl, c *C, schemaID, tableID int64, tp model.ActionType,
	args []interface{}, state *model.SchemaState) *model.Job {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	// TODO: Add the detail error check.
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job, state)

	return job
}

func doDDLJobErr(c *C, schemaID, tableID int64, tp model.ActionType, args []interface{},
	ctx sessionctx.Context, d *ddl) *model.Job {
	return doDDLJobErrWithSchemaState(ctx, d, c, schemaID, tableID, tp, args, nil)
}

func checkCancelState(txn kv.Transaction, job *model.Job, test *testCancelJob) error {
	var checkErr error
	addIndexFirstReorg := test.act == model.ActionAddIndex && job.SchemaState == model.StateWriteReorganization && job.SnapshotVer == 0
	// If the action is adding index and the state is writing reorganization, it wants to test the case of cancelling the job when backfilling indexes.
	// When the job satisfies this case of addIndexFirstReorg, the worker hasn't started to backfill indexes.
	if test.cancelState == job.SchemaState && !addIndexFirstReorg {
		if job.SchemaState == model.StateNone && job.State != model.JobStateDone {
			// If the schema state is none, we only test the job is finished.
		} else {
			errs, err := admin.CancelJobs(txn, test.jobIDs)
			if err != nil {
				checkErr = errors.Trace(err)
				return checkErr
			}
			// It only tests cancel one DDL job.
			if errs[0] != test.cancelRetErrs[0] {
				checkErr = errors.Trace(errs[0])
				return checkErr
			}
		}
	}
	return checkErr
}

type testCancelJob struct {
	act           model.ActionType // act is the job action.
	jobIDs        []int64
	cancelRetErrs []error // cancelRetErrs is the first return value of CancelJobs.
	cancelState   model.SchemaState
	ddlRetErr     error
}

func buildCancelJobTests(firstID int64) []testCancelJob {
	err := errCancelledDDLJob
	errs := []error{err}
	noErrs := []error{nil}
	tests := []testCancelJob{
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 1}, cancelRetErrs: errs, cancelState: model.StateDeleteOnly, ddlRetErr: err},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 2}, cancelRetErrs: errs, cancelState: model.StateWriteOnly, ddlRetErr: err},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 3}, cancelRetErrs: errs, cancelState: model.StateWriteReorganization, ddlRetErr: err},
		{act: model.ActionAddIndex, jobIDs: []int64{firstID + 4}, cancelRetErrs: noErrs, cancelState: model.StatePublic, ddlRetErr: err},

		{act: model.ActionDropIndex, jobIDs: []int64{firstID + 5}, cancelRetErrs: errs, cancelState: model.StateWriteOnly, ddlRetErr: err},
		{act: model.ActionDropIndex, jobIDs: []int64{firstID + 6}, cancelRetErrs: errs, cancelState: model.StateDeleteOnly, ddlRetErr: err},
		{act: model.ActionDropIndex, jobIDs: []int64{firstID + 7}, cancelRetErrs: errs, cancelState: model.StateDeleteReorganization, ddlRetErr: err},
		{act: model.ActionDropIndex, jobIDs: []int64{firstID + 8}, cancelRetErrs: noErrs, cancelState: model.StateNone, ddlRetErr: err},

		{act: model.ActionCreateTable, jobIDs: []int64{firstID + 9}, cancelRetErrs: noErrs, cancelState: model.StatePublic, ddlRetErr: err},
	}

	return tests
}

func (s *testDDLSuite) TestCancelJob(c *C) {
	store := testCreateStore(c, "test_cancel_job")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	dbInfo := testSchemaInfo(c, d, "test_cancel_job")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

	// create table t (c1 int, c2 int);
	tblInfo := testTableInfo(c, d, "t", 2)
	ctx := testNewContext(d)
	err := ctx.NewTxn()
	c.Assert(err, IsNil)
	job := testCreateTable(c, ctx, d, dbInfo, tblInfo)
	// insert t values (1, 2);
	originTable := testGetTable(c, d, dbInfo.ID, tblInfo.ID)
	row := types.MakeDatums(1, 2)
	_, err = originTable.AddRecord(ctx, row, false)
	c.Assert(err, IsNil)
	err = ctx.Txn().Commit(context.Background())
	c.Assert(err, IsNil)

	tc := &TestDDLCallback{}
	// set up hook
	firstJobID := job.ID
	tests := buildCancelJobTests(firstJobID)
	var checkErr error
	var test *testCancelJob
	tc.onJobUpdated = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		hookCtx := mock.NewContext()
		hookCtx.Store = store
		var err error
		err = hookCtx.NewTxn()
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
		checkCancelState(hookCtx.Txn(), job, test)
		err = hookCtx.Txn().Commit(context.Background())
		if err != nil {
			checkErr = errors.Trace(err)
			return
		}
	}
	d.SetHook(tc)

	// for adding index
	test = &tests[0]
	validArgs := []interface{}{false, model.NewCIStr("idx"),
		[]*ast.IndexPartSpecification{{
			Column: &ast.ColumnName{Name: model.NewCIStr("c1")},
			Length: -1,
		}}, nil}
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &test.cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[1]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &test.cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[2]
	// When the job satisfies this test case, the option will be rollback, so the job's schema state is none.
	cancelState := model.StateNone
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, validArgs, &cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[3]
	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "idx", "c2")
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	c.Assert(ctx.Txn().Commit(context.Background()), IsNil)

	// for dropping index
	idxName := []interface{}{model.NewCIStr("idx")}
	test = &tests[4]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, idxName, &test.cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[5]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, idxName, &test.cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[6]
	doDDLJobErrWithSchemaState(ctx, d, c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, idxName, &test.cancelState)
	c.Check(errors.ErrorStack(checkErr), Equals, "")
	test = &tests[7]
	testDropIndex(c, ctx, d, dbInfo, tblInfo, "idx")
	c.Check(errors.ErrorStack(checkErr), Equals, "")

	// for creating table
	test = &tests[8]
	tblInfo = testTableInfo(c, d, "t1", 3)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)
}

func (s *testDDLSuite) TestIgnorableSpec(c *C) {
	specs := []ast.AlterTableType{
		ast.AlterTableOption,
		ast.AlterTableAddColumns,
		ast.AlterTableAddConstraint,
		ast.AlterTableDropColumn,
		ast.AlterTableDropPrimaryKey,
		ast.AlterTableDropIndex,
		ast.AlterTableDropForeignKey,
		ast.AlterTableModifyColumn,
		ast.AlterTableChangeColumn,
		ast.AlterTableRenameTable,
		ast.AlterTableAlterColumn,
	}
	for _, spec := range specs {
		c.Assert(isIgnorableSpec(spec), IsFalse)
	}

	ignorableSpecs := []ast.AlterTableType{
		ast.AlterTableLock,
		ast.AlterTableAlgorithm,
	}
	for _, spec := range ignorableSpecs {
		c.Assert(isIgnorableSpec(spec), IsTrue)
	}
}

func (s *testDDLSuite) TestBuildJobDependence(c *C) {
	store := testCreateStore(c, "test_set_job_relation")
	defer store.Close()

	// Add some non-add-index jobs.
	job1 := &model.Job{ID: 1, TableID: 1, Type: model.ActionAddColumn}
	job2 := &model.Job{ID: 2, TableID: 1, Type: model.ActionCreateTable}
	job3 := &model.Job{ID: 3, TableID: 2, Type: model.ActionDropColumn}
	job6 := &model.Job{ID: 6, TableID: 1, Type: model.ActionDropTable}
	job7 := &model.Job{ID: 7, TableID: 2, Type: model.ActionModifyColumn}
	job9 := &model.Job{ID: 9, SchemaID: 111, Type: model.ActionDropSchema}
	job11 := &model.Job{ID: 11, TableID: 2, Type: model.ActionRenameTable, Args: []interface{}{int64(111), "old db name"}}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.EnQueueDDLJob(job1)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job2)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job3)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job6)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job7)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job9)
		c.Assert(err, IsNil)
		err = t.EnQueueDDLJob(job11)
		c.Assert(err, IsNil)
		return nil
	})
	job4 := &model.Job{ID: 4, TableID: 1, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job4)
		c.Assert(err, IsNil)
		c.Assert(job4.DependencyID, Equals, int64(2))
		return nil
	})
	job5 := &model.Job{ID: 5, TableID: 2, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job5)
		c.Assert(err, IsNil)
		c.Assert(job5.DependencyID, Equals, int64(3))
		return nil
	})
	job8 := &model.Job{ID: 8, TableID: 3, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job8)
		c.Assert(err, IsNil)
		c.Assert(job8.DependencyID, Equals, int64(0))
		return nil
	})
	job10 := &model.Job{ID: 10, SchemaID: 111, TableID: 3, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job10)
		c.Assert(err, IsNil)
		c.Assert(job10.DependencyID, Equals, int64(9))
		return nil
	})
	job12 := &model.Job{ID: 12, SchemaID: 112, TableID: 2, Type: model.ActionAddIndex}
	kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := buildJobDependence(t, job12)
		c.Assert(err, IsNil)
		c.Assert(job12.DependencyID, Equals, int64(11))
		return nil
	})
}

func (s *testDDLSuite) TestParallelDDL(c *C) {
	store := testCreateStore(c, "test_parallel_ddl")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)
	err := ctx.NewTxn()
	c.Assert(err, IsNil)

	/*
		build structure:
			DBs -> {
			 db1: test_parallel_ddl_1
			 db2: test_parallel_ddl_2
			}
			Tables -> {
			 db1.t1 (c1 int, c2 int)
			 db1.t2 (c1 int primary key, c2 int, c3 int)
			 db2.t3 (c1 int, c2 int, c3 int, c4 int)
			}
			Data -> {
			 t1: (10, 10), (20, 20)
			 t2: (1, 1, 1), (2, 2, 2), (3, 3, 3)
			 t3: (11, 22, 33, 44)
			}
	*/
	// create database test_parallel_ddl_1;
	dbInfo1 := testSchemaInfo(c, d, "test_parallel_ddl_1")
	testCreateSchema(c, ctx, d, dbInfo1)
	// create table t1 (c1 int, c2 int);
	tblInfo1 := testTableInfo(c, d, "t1", 2)
	testCreateTable(c, ctx, d, dbInfo1, tblInfo1)
	// insert t1 values (10, 10), (20, 20)
	tbl1 := testGetTable(c, d, dbInfo1.ID, tblInfo1.ID)
	_, err = tbl1.AddRecord(ctx, types.MakeDatums(1, 1), false)
	c.Assert(err, IsNil)
	_, err = tbl1.AddRecord(ctx, types.MakeDatums(2, 2), false)
	c.Assert(err, IsNil)
	// create table t2 (c1 int primary key, c2 int, c3 int);
	tblInfo2 := testTableInfo(c, d, "t2", 3)
	tblInfo2.Columns[0].Flag = mysql.PriKeyFlag | mysql.NotNullFlag
	tblInfo2.PKIsHandle = true
	testCreateTable(c, ctx, d, dbInfo1, tblInfo2)
	// insert t2 values (1, 1), (2, 2), (3, 3)
	tbl2 := testGetTable(c, d, dbInfo1.ID, tblInfo2.ID)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(1, 1, 1), false)
	c.Assert(err, IsNil)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(2, 2, 2), false)
	c.Assert(err, IsNil)
	_, err = tbl2.AddRecord(ctx, types.MakeDatums(3, 3, 3), false)
	c.Assert(err, IsNil)
	// create database test_parallel_ddl_2;
	dbInfo2 := testSchemaInfo(c, d, "test_parallel_ddl_2")
	testCreateSchema(c, ctx, d, dbInfo2)
	// create table t3 (c1 int, c2 int, c3 int, c4 int);
	tblInfo3 := testTableInfo(c, d, "t3", 4)
	testCreateTable(c, ctx, d, dbInfo2, tblInfo3)
	// insert t3 values (11, 22, 33, 44)
	tbl3 := testGetTable(c, d, dbInfo2.ID, tblInfo3.ID)
	_, err = tbl3.AddRecord(ctx, types.MakeDatums(11, 22, 33, 44), false)
	c.Assert(err, IsNil)

	// set hook to execute jobs after all jobs are in queue.
	jobCnt := int64(11)
	tc := &TestDDLCallback{}
	once := sync.Once{}
	var checkErr error
	tc.onJobRunBefore = func(job *model.Job) {
		// TODO: extract a unified function for other tests.
		once.Do(func() {
			qLen1 := int64(0)
			qLen2 := int64(0)
			for {
				checkErr = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
					m := meta.NewMeta(txn)
					qLen1, err = m.DDLJobQueueLen()
					if err != nil {
						return err
					}
					qLen2, err = m.DDLJobQueueLen(meta.AddIndexJobListKey)
					if err != nil {
						return err
					}
					return nil
				})
				if checkErr != nil {
					break
				}
				if qLen1+qLen2 == jobCnt {
					if qLen2 != 5 {
						checkErr = errors.Errorf("add index jobs cnt %v != 5", qLen2)
					}
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
	d.SetHook(tc)
	c.Assert(checkErr, IsNil)

	/*
		prepare jobs:
		/	job no.	/	database no.	/	table no.	/	action type	 /
		/     1		/	 	1			/		1		/	add index	 /
		/     2		/	 	1			/		1		/	add column	 /
		/     3		/	 	1			/		1		/	add index	 /
		/     4		/	 	1			/		2		/	drop column	 /
		/     5		/	 	1			/		1		/	drop index 	 /
		/     6		/	 	1			/		2		/	add index	 /
		/     7		/	 	2			/		3		/	drop column	 /
		/     8		/	 	2			/		3		/	rebase autoID/
		/     9		/	 	1			/		1		/	add index	 /
		/     10	/	 	2			/		null   	/	drop schema  /
		/     11	/	 	2			/		2		/	add index	 /
	*/
	job1 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx1", "c1")
	d.addDDLJob(ctx, job1)
	job2 := buildCreateColumnJob(dbInfo1, tblInfo1, "c3", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, nil)
	d.addDDLJob(ctx, job2)
	job3 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx2", "c3")
	d.addDDLJob(ctx, job3)
	job4 := buildDropColumnJob(dbInfo1, tblInfo2, "c3")
	d.addDDLJob(ctx, job4)
	job5 := buildDropIdxJob(dbInfo1, tblInfo1, "db1_idx1")
	d.addDDLJob(ctx, job5)
	job6 := buildCreateIdxJob(dbInfo1, tblInfo2, false, "db2_idx1", "c2")
	d.addDDLJob(ctx, job6)
	job7 := buildDropColumnJob(dbInfo2, tblInfo3, "c4")
	d.addDDLJob(ctx, job7)
	job8 := buildRebaseAutoIDJobJob(dbInfo2, tblInfo3, 1024)
	d.addDDLJob(ctx, job8)
	job9 := buildCreateIdxJob(dbInfo1, tblInfo1, false, "db1_idx3", "c2")
	d.addDDLJob(ctx, job9)
	job10 := buildDropSchemaJob(dbInfo2)
	d.addDDLJob(ctx, job10)
	job11 := buildCreateIdxJob(dbInfo2, tblInfo3, false, "db3_idx1", "c2")
	d.addDDLJob(ctx, job11)
	// TODO: add rename table job

	// check results.
	isChecked := false
	for !isChecked {
		kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			lastJob, err := m.GetHistoryDDLJob(job11.ID)
			c.Assert(err, IsNil)
			// all jobs are finished.
			if lastJob != nil {
				finishedJobs, err := m.GetAllHistoryDDLJobs()
				c.Assert(err, IsNil)
				// get the last 11 jobs completed.
				finishedJobs = finishedJobs[len(finishedJobs)-11:]
				// check some jobs are ordered because of the dependence.
				c.Assert(finishedJobs[0].ID, Equals, job1.ID)
				c.Assert(finishedJobs[1].ID, Equals, job2.ID)
				c.Assert(finishedJobs[2].ID, Equals, job3.ID)
				c.Assert(finishedJobs[4].ID, Equals, job5.ID)
				c.Assert(finishedJobs[10].ID, Equals, job11.ID)
				// check the jobs are ordered in the adding-index-job queue or general-job queue.
				addIdxJobID := int64(0)
				generalJobID := int64(0)
				for _, job := range finishedJobs {
					// check jobs' order.
					if job.Type == model.ActionAddIndex {
						c.Assert(job.ID, Greater, addIdxJobID)
						addIdxJobID = job.ID
					} else {
						c.Assert(job.ID, Greater, generalJobID)
						generalJobID = job.ID
					}
					// check jobs' state.
					if job.ID == lastJob.ID {
						c.Assert(job.State, Equals, model.JobStateCancelled, Commentf("job: %v", job))
					} else {
						c.Assert(job.State, Equals, model.JobStateSynced, Commentf("job: %v", job))
					}
				}

				isChecked = true
			}
			return nil
		})
		time.Sleep(10 * time.Millisecond)
	}

	tc = &TestDDLCallback{}
	d.SetHook(tc)
}

func (s *testDDLSuite) TestDDLPackageExecuteSQL(c *C) {
	store := testCreateStore(c, "test_run_sql")
	defer store.Close()

	RunWorker = true
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	testCheckOwner(c, d, true)
	defer d.Stop()
	worker := d.generalWorker()
	c.Assert(worker, NotNil)

	// In test environment, worker.ctxPool will be nil, and get will return mock.Context.
	// We just test that can use it to call sqlexec.SQLExecutor.Execute.
	sess, err := worker.sessPool.get()
	c.Assert(err, IsNil)
	defer worker.sessPool.put(sess)
	se := sess.(sqlexec.SQLExecutor)
	_, _ = se.Execute(context.Background(), "create table t(a int);")
}
