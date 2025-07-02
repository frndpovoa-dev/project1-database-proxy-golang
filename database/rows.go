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

// Reference: https://github.com/CanonicalLtd/go-grpc-sql/blob/181d263025fb02a680c1726752eb27f3a2154e26/rows.go

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"time"
)

var _ driver.Rows = (*Rows)(nil)

type Rows struct {
	conn   *Conn
	tx     *Tx
	result *QueryResult
	cursor int
}

func (this *Rows) Columns() []string {
	columns := make([]string, 0)
	if len(this.result.Rows) > 0 {
		for i, _ := range this.result.Rows[0].Cols {
			columns = append(columns, fmt.Sprintf("col%d", i))
		}
	}
	return columns
}

func (this *Rows) Close() error {
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
	_, err := databaseProxy.CloseResultSet(ctx, &NextConfig{Transaction: this.tx.transaction, QueryResultId: this.result.Id})
	return err
}

func (this *Rows) Next(dest []driver.Value) error {
	if (this.cursor + 1) > len(this.result.Rows) {
		grpcConn := this.conn.newGrpcConn(this.tx.transaction.Node)
		databaseProxy := NewDatabaseProxyClient(grpcConn)
		defer grpcConn.Close()
		defer databaseProxy.CloseConnection(this.tx.ctx, &Empty{})
		if result, err := databaseProxy.Next(this.tx.ctx, &NextConfig{Transaction: this.tx.transaction, QueryResultId: this.result.Id}); err != nil {
			return err
		} else if len(result.Rows) > 0 {
			this.result = result
			this.cursor = 0
		} else {
			return io.EOF
		}
	}
	if values, err := this.conn.ToDriverValues(this.result.Rows[this.cursor].Cols); err != nil {
		return err
	} else {
		for i, v := range values {
			dest[i] = v
		}
		this.cursor++
		return nil
	}
}
