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
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/infoschema"
	"github.com/hanchuanchuan/goInception/kv"
	"github.com/hanchuanchuan/goInception/meta"
	"github.com/hanchuanchuan/goInception/model"
	"github.com/hanchuanchuan/goInception/sessionctx"
	"github.com/hanchuanchuan/goInception/store/mockstore"
	"github.com/hanchuanchuan/goInception/terror"
	"github.com/hanchuanchuan/goInception/types"
	"github.com/hanchuanchuan/goInception/util"
	"github.com/hanchuanchuan/goInception/util/logutil"
	"github.com/hanchuanchuan/goInception/util/mock"
	. "github.com/pingcap/check"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type DDLForTest interface {
	// SetHook sets the hook.
	SetHook(h Callback)
	// SetInterceptoror sets the interceptor.
	SetInterceptoror(h Interceptor)
}

// SetHook implements DDL.SetHook interface.
func (d *ddl) SetHook(h Callback) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.hook = h
}

// SetInterceptoror implements DDL.SetInterceptoror interface.
func (d *ddl) SetInterceptoror(i Interceptor) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.interceptor = i
}

// generalWorker returns the general worker.
func (d *ddl) generalWorker() *worker {
	return d.workers[generalWorker]
}

// restartWorkers is like the function of d.start. But it won't initialize the "workers" and create a new worker.
// It only starts the original workers.
func (d *ddl) restartWorkers(ctx context.Context) {
	d.quitCh = make(chan struct{})
	if !RunWorker {
		return
	}

	err := d.ownerManager.CampaignOwner(ctx)
	terror.Log(err)
	for _, worker := range d.workers {
		worker.wg.Add(1)
		worker.quitCh = make(chan struct{})
		w := worker
		go util.WithRecovery(func() { w.start(d.ddlCtx) },
			func(r interface{}) {
				if r != nil {
					log.Errorf("[ddl-%s] ddl %s meet panic", w, d.uuid)
				}
			})
		asyncNotify(worker.ddlJobCh)
	}
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level:  logLevel,
		Format: "highlight",
	})
	TestingT(t)
}

func testCreateStore(c *C, name string) kv.Storage {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	return store
}

func testNewContext(d *ddl) sessionctx.Context {
	ctx := mock.NewContext()
	ctx.Store = d.store
	return ctx
}

func testNewDDL(ctx context.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handle, hook Callback, lease time.Duration) *ddl {
	return newDDL(ctx, etcdCli, store, infoHandle, hook, lease, nil)
}

func getSchemaVer(c *C, ctx sessionctx.Context) int64 {
	err := ctx.NewTxn()
	c.Assert(err, IsNil)
	m := meta.NewMeta(ctx.Txn())
	ver, err := m.GetSchemaVersion()
	c.Assert(err, IsNil)
	return ver
}

type historyJobArgs struct {
	ver    int64
	db     *model.DBInfo
	tbl    *model.TableInfo
	tblIDs map[int64]struct{}
}

func checkEqualTable(c *C, t1, t2 *model.TableInfo) {
	c.Assert(t1.ID, Equals, t2.ID)
	c.Assert(t1.Name, Equals, t2.Name)
	c.Assert(t1.Charset, Equals, t2.Charset)
	c.Assert(t1.Collate, Equals, t2.Collate)
	c.Assert(t1.PKIsHandle, DeepEquals, t2.PKIsHandle)
	c.Assert(t1.Comment, DeepEquals, t2.Comment)
	c.Assert(t1.AutoIncID, DeepEquals, t2.AutoIncID)
}

func checkHistoryJob(c *C, job *model.Job) {
	c.Assert(job.State, Equals, model.JobStateSynced)
}

func checkHistoryJobArgs(c *C, ctx sessionctx.Context, id int64, args *historyJobArgs) {
	t := meta.NewMeta(ctx.Txn())
	historyJob, err := t.GetHistoryDDLJob(id)
	c.Assert(err, IsNil)
	c.Assert(historyJob.BinlogInfo.FinishedTS, Greater, uint64(0))

	if args.tbl != nil {
		c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
		checkEqualTable(c, historyJob.BinlogInfo.TableInfo, args.tbl)
		return
	}

	// for handling schema job
	c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
	c.Assert(historyJob.BinlogInfo.DBInfo, DeepEquals, args.db)
	// only for creating schema job
	if args.db != nil && len(args.tblIDs) == 0 {
		return
	}
}

func buildCreateIdxJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args: []interface{}{unique, model.NewCIStr(indexName),
			[]*ast.IndexPartSpecification{{
				Column: &ast.ColumnName{Name: model.NewCIStr(colName)},
				Length: types.UnspecifiedLength}}},
	}
}

func testCreateIndex(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	job := buildCreateIdxJob(dbInfo, tblInfo, unique, indexName, colName)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildDropIdxJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{model.NewCIStr(indexName)},
	}
}

func testDropIndex(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	job := buildDropIdxJob(dbInfo, tblInfo, indexName)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildRebaseAutoIDJobJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, newBaseID int64) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionRebaseAutoID,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newBaseID},
	}
}
