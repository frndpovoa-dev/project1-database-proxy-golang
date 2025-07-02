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

// Reference: https://github.com/CanonicalLtd/go-grpc-sql/blob/master/conn.go

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/frndpovoa-dev/project1-database-proxy-golang/stack"
	"google.golang.org/protobuf/proto"
	"reflect"
	"time"
)

var _ driver.Conn = (*Conn)(nil)
var _ driver.ConnBeginTx = (*Conn)(nil)

type Conn struct {
	node        string
	newGrpcConn NewGrpcConn
	txs         *stack.Stack[*Tx]
}

func (this *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{
		conn:  this,
		query: query,
	}, nil
}

func (this *Conn) ExecContext(ctx context.Context, query string, namedArgs []driver.NamedValue) (driver.Result, error) {
	args := make([]driver.Value, 0)
	for _, namedArg := range namedArgs {
		args = append(args, namedArg.Value)
	}
	if tx, ok := this.txs.Peek(); !ok {
		return this.ExecNoTx(ctx, query, args)
	} else {
		return this.ExecTx(tx, query, args)
	}
}

func (this *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if tx, ok := this.txs.Peek(); !ok {
		var timeout int64 = 10_000
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
		defer cancel()
		return this.ExecNoTx(ctx, query, args)
	} else {
		return this.ExecTx(tx, query, args)
	}
}

func (this *Conn) ExecNoTx(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	var timeout int64
	if deadline, ok := ctx.Deadline(); !ok {
		timeout = 10_000
	} else {
		timeout = time.Until(deadline).Milliseconds()
	}
	if argValues, err := this.FromDriverValues(args); err != nil {
		return nil, err
	} else {
		grpcConn := this.newGrpcConn(this.node)
		databaseProxy := NewDatabaseProxyClient(grpcConn)
		defer grpcConn.Close()
		defer databaseProxy.CloseConnection(ctx, &Empty{})
		if result, err := databaseProxy.Execute(ctx, &ExecuteConfig{
			Query:   query,
			Args:    argValues,
			Timeout: timeout,
		}); err != nil {
			return nil, err
		} else {
			return &Result{
				rowsAffected: result.RowsAffected,
			}, nil
		}
	}
}

func (this *Conn) ExecTx(tx *Tx, query string, args []driver.Value) (driver.Result, error) {
	var timeout int64
	if deadline, ok := tx.ctx.Deadline(); !ok {
		timeout = 10_000
	} else {
		timeout = time.Until(deadline).Milliseconds()
	}
	if argValues, err := this.FromDriverValues(args); err != nil {
		return nil, err
	} else {
		grpcConn := this.newGrpcConn(tx.transaction.Node)
		databaseProxy := NewDatabaseProxyClient(grpcConn)
		defer grpcConn.Close()
		defer databaseProxy.CloseConnection(tx.ctx, &Empty{})
		if result, err := databaseProxy.ExecuteTx(tx.ctx, &ExecuteTxConfig{
			Transaction: tx.transaction,
			ExecuteConfig: &ExecuteConfig{
				Query:   query,
				Args:    argValues,
				Timeout: timeout,
			},
		}); err != nil {
			return nil, err
		} else {
			return &Result{
				rowsAffected: result.RowsAffected,
			}, nil
		}
	}
}

func (this *Conn) QueryContext(ctx context.Context, query string, namedArgs []driver.NamedValue) (driver.Rows, error) {
	args := make([]driver.Value, 0)
	for _, namedArg := range namedArgs {
		args = append(args, namedArg.Value)
	}
	if tx, ok := this.txs.Peek(); !ok {
		return this.QueryNoTx(ctx, query, args)
	} else {
		return this.QueryTx(tx, query, args)
	}
}

func (this *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if tx, ok := this.txs.Peek(); !ok {
		var timeout int64 = 10_000
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
		defer cancel()
		return this.QueryNoTx(ctx, query, args)
	} else {
		return this.QueryTx(tx, query, args)
	}
}

func (this *Conn) QueryNoTx(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	var timeout int64
	if deadline, ok := ctx.Deadline(); !ok {
		timeout = 10_000
	} else {
		timeout = time.Until(deadline).Milliseconds()
	}
	if argValues, err := this.FromDriverValues(args); err != nil {
		return nil, err
	} else {
		grpcConn := this.newGrpcConn(this.node)
		databaseProxy := NewDatabaseProxyClient(grpcConn)
		defer grpcConn.Close()
		defer databaseProxy.CloseConnection(ctx, &Empty{})
		if result, err := databaseProxy.Query(ctx, &QueryConfig{
			Query:   query,
			Args:    argValues,
			Timeout: timeout,
		}); err != nil {
			return nil, err
		} else {
			return &Rows{
				conn:   this,
				result: result,
			}, nil
		}
	}
}

func (this *Conn) QueryTx(tx *Tx, query string, args []driver.Value) (driver.Rows, error) {
	var timeout int64
	if deadline, ok := tx.ctx.Deadline(); !ok {
		timeout = 10_000
	} else {
		timeout = time.Until(deadline).Milliseconds()
	}
	if argValues, err := this.FromDriverValues(args); err != nil {
		return nil, err
	} else {
		grpcConn := this.newGrpcConn(tx.transaction.Node)
		databaseProxy := NewDatabaseProxyClient(grpcConn)
		defer grpcConn.Close()
		defer databaseProxy.CloseConnection(tx.ctx, &Empty{})
		if result, err := databaseProxy.QueryTx(tx.ctx, &QueryTxConfig{
			Transaction: tx.transaction,
			QueryConfig: &QueryConfig{
				Query:   query,
				Args:    argValues,
				Timeout: timeout,
			},
		}); err != nil {
			return nil, err
		} else {
			return &Rows{
				conn:   this,
				tx:     tx,
				result: result,
			}, nil
		}
	}
}

func (this *Conn) Close() error {
	return nil
}

func (this *Conn) Begin() (driver.Tx, error) {
	return this.BeginTx(context.Background(), driver.TxOptions{
		ReadOnly: true,
	})
}

func (this *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var timeout int64
	if deadline, ok := ctx.Deadline(); !ok {
		timeout = 10_000
	} else {
		timeout = time.Until(deadline).Milliseconds()
	}
	grpcConn := this.newGrpcConn(this.node)
	databaseProxy := NewDatabaseProxyClient(grpcConn)
	defer grpcConn.Close()
	defer databaseProxy.CloseConnection(ctx, &Empty{})
	if transaction, err := databaseProxy.BeginTransaction(ctx, &BeginTransactionConfig{
		ReadOnly: opts.ReadOnly,
		Timeout:  timeout,
	}); err != nil {
		return nil, err
	} else {
		tx := &Tx{
			ctx:         ctx,
			conn:        this,
			transaction: transaction,
		}
		this.txs.Push(tx)
		return tx, nil
	}
}

func (this *Conn) Commit() error {
	return fmt.Errorf("oppertion not supported, use CommitTx instead")
}

func (this *Conn) CommitTx(tx *Tx) error {
	grpcConn := this.newGrpcConn(tx.transaction.Node)
	databaseProxy := NewDatabaseProxyClient(grpcConn)
	defer grpcConn.Close()
	defer databaseProxy.CloseConnection(tx.ctx, &Empty{})
	if result, err := databaseProxy.CommitTransaction(tx.ctx, tx.transaction); err != nil {
		return err
	} else {
		if result.Status != Transaction_COMMITTED {
			return fmt.Errorf("commit failed")
		} else {
			this.txs.Pop()
			return nil
		}
	}
}

func (this *Conn) Rollback() error {
	return fmt.Errorf("oppertion not supported, use RollbackTx instead")
}

func (this *Conn) RollbackTx(tx *Tx) error {
	grpcConn := this.newGrpcConn(tx.transaction.Node)
	databaseProxy := NewDatabaseProxyClient(grpcConn)
	defer grpcConn.Close()
	defer databaseProxy.CloseConnection(tx.ctx, &Empty{})
	if result, err := databaseProxy.RollbackTransaction(tx.ctx, tx.transaction); err != nil {
		return err
	} else {
		if result.Status != Transaction_ROLLED_BACK {
			return fmt.Errorf("rollback failed")
		} else {
			this.txs.Pop()
			return nil
		}
	}
}

func (this *Conn) FromDriverValues(objects []driver.Value) ([]*Value, error) {
	values := make([]*Value, len(objects))
	for i, object := range objects {
		if value, err := toValue(object); err != nil {
			return nil, fmt.Errorf("cannot convert %d to value (%v). cause is: %s", i, object, err)
		} else {
			values[i] = value
		}
	}
	return values, nil
}

func toValue(value interface{}) (*Value, error) {
	var code ValueCode
	var message proto.Message

	switch v := value.(type) {
	case int64:
		code = ValueCode_INT64
		message = &ValueInt64{Value: v}
	case float64:
		code = ValueCode_FLOAT64
		message = &ValueFloat64{Value: v}
	case bool:
		code = ValueCode_BOOL
		message = &ValueBool{Value: v}
	case string:
		code = ValueCode_STRING
		message = &ValueString{Value: v}
	default:
		if value != nil {
			return nil, fmt.Errorf("invalid type %s", reflect.TypeOf(value).Kind())
		}
		code = ValueCode_NULL
		message = &ValueNull{}
	}

	if data, err := proto.Marshal(message); err != nil {
		return nil, err
	} else {
		return &Value{
			Code: code,
			Data: data,
		}, nil
	}
}

func (this *Conn) ToDriverValues(values []*Value) ([]driver.Value, error) {
	args, err := fromValueSlice(values)
	if err != nil {
		return nil, err
	}
	a := make([]driver.Value, len(args))
	for i, arg := range args {
		a[i] = arg
	}
	return a, nil
}

func fromValueSlice(values []*Value) ([]interface{}, error) {
	objects := make([]interface{}, len(values))
	for i, value := range values {
		object, err := fromValue(value)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal value %d: %s", i, err)
		}
		objects[i] = object
	}
	return objects, nil
}

func fromValue(value *Value) (interface{}, error) {
	var message ValueMessage
	switch value.Code {
	case ValueCode_INT32:
		message = &ValueInt32{}
	case ValueCode_INT64:
		message = &ValueInt64{}
	case ValueCode_FLOAT64:
		message = &ValueFloat64{}
	case ValueCode_BOOL:
		message = &ValueBool{}
	case ValueCode_STRING:
		message = &ValueString{}
	case ValueCode_TIME:
		message = &ValueTime{}
	case ValueCode_NULL:
		message = &ValueNull{}
	default:
		return nil, fmt.Errorf("invalid value type code %d", value.Code)
	}

	if err := proto.Unmarshal(value.Data, message); err != nil {
		return nil, err
	} else {
		return message.Interface(), nil
	}
}

type ValueMessage interface {
	proto.Message
	Interface() interface{}
}

func (v *ValueInt32) Interface() interface{} {
	return v.Value
}

func (v *ValueInt64) Interface() interface{} {
	return v.Value
}

func (v *ValueFloat64) Interface() interface{} {
	return v.Value
}

func (v *ValueBool) Interface() interface{} {
	return v.Value
}

func (v *ValueString) Interface() interface{} {
	return v.Value
}

func (v *ValueTime) Interface() interface{} {
	return v.Value
}

func (v *ValueNull) Interface() interface{} {
	return nil
}
