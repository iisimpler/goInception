// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

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

package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"

	// For pprof
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blacktear23/go-proxyprotocol"
	"github.com/hanchuanchuan/goInception/config"
	"github.com/hanchuanchuan/goInception/mysql"
	"github.com/hanchuanchuan/goInception/sessionctx/variable"
	"github.com/hanchuanchuan/goInception/terror"
	"github.com/hanchuanchuan/goInception/util"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
)

var (
	baseConnID uint32
)

var (
	errUnknownFieldType  = terror.ClassServer.New(codeUnknownFieldType, "unknown field type")
	errInvalidPayloadLen = terror.ClassServer.New(codeInvalidPayloadLen, "invalid payload length")
	errInvalidSequence   = terror.ClassServer.New(codeInvalidSequence, "invalid sequence")
	errInvalidType       = terror.ClassServer.New(codeInvalidType, "invalid type")
	errNotAllowedCommand = terror.ClassServer.New(codeNotAllowedCommand, "the used command is not allowed with this TiDB version")
	errAccessDenied      = terror.ClassServer.New(codeAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDenied])
)

// DefaultCapability is the capability of the server when it is created using the default configuration.
// When server is configured with SSL, the server will have extra capabilities compared to DefaultCapability.
const defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts | mysql.ClientPluginAuth

// Server is the MySQL protocol server
type Server struct {
	cfg               *config.Config
	tlsConfig         *tls.Config
	driver            IDriver
	listener          net.Listener
	rwlock            *sync.RWMutex
	concurrentLimiter *TokenLimiter
	clients           map[uint32]*clientConn
	capability        uint32

	// stopListenerCh is used when a critical error occurred, we don't want to exit the process, because there may be
	// a supervisor automatically restart it, then new client connection will be created, but we can't server it.
	// So we just stop the listener and store to force clients to chose other TiDB servers.
	stopListenerCh chan struct{}
	statusServer   *http.Server

	// osc进程列表
	oscProcessList map[string]*util.OscProcessInfo
}

// ConnectionCount gets current connection count.
func (s *Server) ConnectionCount() int {
	var cnt int
	s.rwlock.RLock()
	cnt = len(s.clients)
	s.rwlock.RUnlock()
	return cnt
}

func (s *Server) getToken() *Token {
	tok := s.concurrentLimiter.Get()
	// Note that data smaller than one microsecond is ignored, because that case can be viewed as non-block.
	return tok
}

func (s *Server) releaseToken(token *Token) {
	s.concurrentLimiter.Put(token)
}

// newConn creates a new *clientConn from a net.Conn.
// It allocates a connection ID and random salt data for authentication.
func (s *Server) newConn(conn net.Conn) *clientConn {
	cc := newClientConn(s)
	if s.cfg.Performance.TCPKeepAlive {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				log.Error("failed to set tcp keep alive option:", err)
			}
		}
	}
	cc.setConn(conn)
	cc.salt = util.RandomBuf(20)
	return cc
}

func (s *Server) skipAuth() bool {
	return s.cfg.Socket != ""
}

func (s *Server) InitOscProcessList() {
	if s.oscProcessList == nil {
		s.oscProcessList = make(map[string]*util.OscProcessInfo)
	}
}

// NewServer creates a new Server.
func NewServer(cfg *config.Config, driver IDriver) (*Server, error) {
	s := &Server{
		cfg:               cfg,
		driver:            driver,
		concurrentLimiter: NewTokenLimiter(cfg.TokenLimit),
		rwlock:            &sync.RWMutex{},
		clients:           make(map[uint32]*clientConn),
		stopListenerCh:    make(chan struct{}, 1),

		oscProcessList: make(map[string]*util.OscProcessInfo),
	}
	s.loadTLSCertificates()

	s.capability = defaultCapability
	if s.tlsConfig != nil {
		s.capability |= mysql.ClientSSL
	}

	var err error
	if cfg.Socket != "" {
		if s.listener, err = net.Listen("unix", cfg.Socket); err == nil {
			log.Infof("Server is running MySQL Protocol through Socket [%s]", cfg.Socket)
		}
	} else {
		addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
		if s.listener, err = net.Listen("tcp", addr); err == nil {
			log.Infof("Server is running MySQL Protocol at [%s]", addr)
		}
	}

	if cfg.ProxyProtocol.Networks != "" {
		pplistener, errProxy := proxyprotocol.NewListener(s.listener, cfg.ProxyProtocol.Networks,
			int(cfg.ProxyProtocol.HeaderTimeout))
		if errProxy != nil {
			log.Error("ProxyProtocol Networks parameter invalid")
			return nil, errors.Trace(errProxy)
		}
		log.Infof("Server is running MySQL Protocol (through PROXY Protocol) at [%s]", s.cfg.Host)
		s.listener = pplistener
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())
	return s, nil
}

func (s *Server) loadTLSCertificates() {
	defer func() {
		if s.tlsConfig != nil {
			log.Infof("Secure connection is enabled (client verification enabled = %v)", len(variable.SysVars["ssl_ca"].Value) > 0)
			variable.SysVars["have_openssl"].Value = "YES"
			variable.SysVars["have_ssl"].Value = "YES"
			variable.SysVars["ssl_cert"].Value = s.cfg.Security.SSLCert
			variable.SysVars["ssl_key"].Value = s.cfg.Security.SSLKey
		} else {
			log.Info("Secure connection is NOT ENABLED")
		}
	}()

	if len(s.cfg.Security.SSLCert) == 0 || len(s.cfg.Security.SSLKey) == 0 {
		s.tlsConfig = nil
		return
	}

	tlsCert, err := tls.LoadX509KeyPair(s.cfg.Security.SSLCert, s.cfg.Security.SSLKey)
	if err != nil {
		log.Warn(errors.ErrorStack(err))
		s.tlsConfig = nil
		return
	}

	// Try loading CA cert.
	clientAuthPolicy := tls.NoClientCert
	var certPool *x509.CertPool
	if len(s.cfg.Security.SSLCA) > 0 {
		caCert, err := os.ReadFile(s.cfg.Security.SSLCA)
		if err != nil {
			log.Warn(errors.ErrorStack(err))
		} else {
			certPool = x509.NewCertPool()
			if certPool.AppendCertsFromPEM(caCert) {
				clientAuthPolicy = tls.VerifyClientCertIfGiven
			}
			variable.SysVars["ssl_ca"].Value = s.cfg.Security.SSLCA
		}
	}
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuthPolicy,
		MinVersion:   0,
	}
}

// Run runs the server.
func (s *Server) Run() error {

	// Start HTTP API to report tidb info such as TPS.
	if s.cfg.Status.ReportStatus {
		s.startStatusHTTP()
	}
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
			}

			// If we got PROXY protocol error, we should continue accept.
			if proxyprotocol.IsProxyProtocolError(err) {
				log.Errorf("PROXY protocol error: %s", err.Error())
				continue
			}

			log.Errorf("accept error %s", err.Error())
			return errors.Trace(err)
		}
		if s.shouldStopListener() {
			err = conn.Close()
			terror.Log(errors.Trace(err))
			break
		}
		go s.onConn(conn)
	}
	err := s.listener.Close()
	terror.Log(errors.Trace(err))
	s.listener = nil
	for {
		log.Errorf("listener stopped, waiting for manual kill.")
		time.Sleep(time.Minute)
	}
}

func (s *Server) shouldStopListener() bool {
	select {
	case <-s.stopListenerCh:
		return true
	default:
		return false
	}
}

// Close closes the server.
func (s *Server) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	if s.listener != nil {
		err := s.listener.Close()
		terror.Log(errors.Trace(err))
		s.listener = nil
	}
	if s.statusServer != nil {
		err := s.statusServer.Close()
		terror.Log(errors.Trace(err))
		s.statusServer = nil
	}
}

// onConn runs in its own goroutine, handles queries from this connection.
func (s *Server) onConn(c net.Conn) {
	conn := s.newConn(c)
	if err := conn.handshake(); err != nil {
		// Some keep alive services will send request to TiDB and disconnect immediately.
		// So we only record metrics.
		err = c.Close()
		terror.Log(errors.Trace(err))
		return
	}
	log.Infof("con:%d new connection %s", conn.connectionID, c.RemoteAddr().String())
	defer func() {
		log.Infof("con:%d close connection", conn.connectionID)
	}()
	s.rwlock.Lock()
	s.clients[conn.connectionID] = conn
	s.rwlock.Unlock()

	conn.Run()
}

// ShowProcessList implements the SessionManager interface.
func (s *Server) ShowProcessList() map[uint64]util.ProcessInfo {
	s.rwlock.RLock()
	rs := make(map[uint64]util.ProcessInfo, len(s.clients))
	for _, client := range s.clients {
		if atomic.LoadInt32(&client.status) == connStatusWaitShutdown {
			continue
		}
		pi := client.ctx.ShowProcess()
		rs[pi.ID] = pi
	}
	s.rwlock.RUnlock()
	return rs
}

// Kill implements the SessionManager interface.
func (s *Server) Kill(connectionID uint64, query bool) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	log.Infof("[server] Kill connectionID %d, query %t]", connectionID, query)

	conn, ok := s.clients[uint32(connectionID)]
	if !ok {
		return
	}

	conn.mu.RLock()
	cancelFunc := conn.mu.cancelFunc
	conn.mu.RUnlock()
	if cancelFunc != nil {
		cancelFunc()
	}

	if !query {
		// 带 query 是 kill 掉正在执行中的请求
		// 不带 query 是 kill 掉 session 连接
		// Mark the client connection status as WaitShutdown, when the goroutine detect
		// this, it will end the dispatch loop and exit.
		atomic.StoreInt32(&conn.status, connStatusWaitShutdown)
	}
}

// GracefulDown waits all clients to close.
func (s *Server) GracefulDown() {
	log.Info("[server] graceful shutdown.")

	count := s.ConnectionCount()
	for i := 0; count > 0; i++ {
		time.Sleep(time.Second)
		s.kickIdleConnection()

		count = s.ConnectionCount()
		// Print information for every 30s.
		if i%30 == 0 {
			log.Infof("graceful shutdown...connection count %d\n", count)
		}
	}
}

func (s *Server) kickIdleConnection() {
	var conns []*clientConn
	s.rwlock.RLock()
	for _, cc := range s.clients {
		if cc.ShutdownOrNotify() {
			// Shutdowned conn will be closed by us, and notified conn will exist themselves.
			conns = append(conns, cc)
		}
	}
	s.rwlock.RUnlock()

	for _, cc := range conns {
		err := cc.Close()
		if err != nil {
			log.Error("close connection error:", err)
		}
	}
}

// AddOscProcess 添加osc进程
func (s *Server) AddOscProcess(p *util.OscProcessInfo) {
	if s.rwlock == nil {
		s.rwlock = &sync.RWMutex{}
		s.oscProcessList = make(map[string]*util.OscProcessInfo)
	}
	s.rwlock.Lock()
	s.oscProcessList[p.Sqlsha1] = p
	s.rwlock.Unlock()
}

// KillOscProcess Kill当前会话的Osc进程
func (s *Server) KillOscProcess(connectionID uint64) {
	if s.rwlock == nil {
		s.rwlock = &sync.RWMutex{}
		s.oscProcessList = make(map[string]*util.OscProcessInfo)
		return
	}
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	// 执行完成或中止后清理osc进程信息
	pl := s.oscProcessList
	if len(pl) == 0 {
		return
	}
	oscList := []string{}
	for _, pi := range pl {
		if pi.ConnID == connectionID {
			oscList = append(oscList, pi.Sqlsha1)
		}
	}

	if len(oscList) > 0 {
		for _, sha1 := range oscList {
			delete(pl, sha1)
		}
	}
}

func (s *Server) OscLock() {
	if s.rwlock == nil {
		s.rwlock = &sync.RWMutex{}
	}
	s.rwlock.Lock()
}

func (s *Server) OscUnLock() {
	s.rwlock.Unlock()
}

// ShowOscProcessListWithWrite 返回osc进程列表用于暂停等操作
func (s *Server) ShowOscProcessListWithWrite() map[string]*util.OscProcessInfo {
	// if s.rwlock == nil {
	// 	s.rwlock = &sync.RWMutex{}
	// 	s.oscProcessList = make(map[string]*util.OscProcessInfo)
	// }
	// s.rwlock.RLock()
	// defer s.rwlock.RUnlock()
	return s.oscProcessList
}

// ShowOscProcessList 返回只读的osc进程列表
func (s *Server) ShowOscProcessList() map[string]*util.OscProcessInfo {
	if s.rwlock == nil {
		s.rwlock = &sync.RWMutex{}
		s.oscProcessList = make(map[string]*util.OscProcessInfo)
	}
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()

	rs := make(map[string]*util.OscProcessInfo, len(s.oscProcessList))
	for key, client := range s.oscProcessList {
		client.RW.Lock()
		pi := util.OscProcessInfo{
			ID:         client.ID,
			ConnID:     client.ConnID,
			Schema:     client.Schema,
			Table:      client.Table,
			Command:    client.Command,
			Sqlsha1:    client.Sqlsha1,
			Percent:    client.Percent,
			RemainTime: client.RemainTime,
			Info:       client.Info,
			Killed:     client.Killed,
			IsGhost:    client.IsGhost,
			Pause:      client.Pause,
		}
		client.RW.Unlock()
		rs[key] = &pi
	}
	return rs
}

// Server error codes.
const (
	codeUnknownFieldType  = 1
	codeInvalidPayloadLen = 2
	codeInvalidSequence   = 3
	codeInvalidType       = 4

	codeNotAllowedCommand = 1148
	codeAccessDenied      = mysql.ErrAccessDenied
)

func init() {
	serverMySQLErrCodes := map[terror.ErrCode]uint16{
		codeNotAllowedCommand: mysql.ErrNotAllowedCommand,
		codeAccessDenied:      mysql.ErrAccessDenied,
	}
	terror.ErrClassToMySQLCodes[terror.ClassServer] = serverMySQLErrCodes
}
