package database

/*-
 * #%L
 * database-proxy
 * %%
 * Copyright (C) 2025 Fernando Lemes Povoa
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import (
	"context"
	"database/sql/driver"
	"time"
)

var _ driver.Stmt = (*Stmt)(nil)

type Stmt struct {
	conn  *Conn
	tx    *Tx
	query string
}

func (this *Stmt) Close() error {
	var timeout int64 = 5_000
	var ctx context.Context
	if this.tx == nil {
		ctx1, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
		defer cancel()
		ctx = ctx1
	} else {
		ctx = this.tx.ctx
	}
	grpcConn := this.conn.newGrpcConn(this.tx.transaction.Node)
	databaseProxy := NewDatabaseProxyClient(grpcConn)
	defer grpcConn.Close()
	defer databaseProxy.CloseConnection(this.tx.ctx, &Empty{})
	_, err := databaseProxy.CloseStatement(ctx, &Empty{})
	return err
}

func (this *Stmt) NumInput() int {
	return -1
}

func (this *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return this.conn.Exec(this.query, args)
}

func (this *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return this.conn.Query(this.query, args)
}
