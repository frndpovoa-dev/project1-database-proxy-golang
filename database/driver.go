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

// Reference: https://github.com/CanonicalLtd/go-grpc-sql/blob/master/driver.go

import (
	"database/sql/driver"
	"github.com/frndpovoa-dev/project1-database-proxy-golang/stack"
	"google.golang.org/grpc"
)

var _ driver.Driver = (*Driver)(nil)

type Driver struct {
	newGrpcConn NewGrpcConn
}

type DriverConfig struct {
	NewGrpcConn NewGrpcConn
}

type NewGrpcConn func(node string) *grpc.ClientConn

func NewDriver(config *DriverConfig) *Driver {
	return (&Driver{
		newGrpcConn: config.NewGrpcConn,
	}).SetDefaultValues()
}

func (this *Driver) SetDefaultValues() *Driver {
	return this
}

func (this *Driver) Open(node string) (driver.Conn, error) {
	return &Conn{
		node:        node,
		newGrpcConn: this.newGrpcConn,
		txs:         stack.NewStack[*Tx](),
	}, nil
}
