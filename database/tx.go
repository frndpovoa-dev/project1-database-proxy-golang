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
)

// Reference: https://github.com/CanonicalLtd/go-grpc-sql/blob/master/tx.go

var _ driver.Tx = (*Tx)(nil)

type Tx struct {
	ctx         context.Context
	conn        *Conn
	transaction *Transaction
}

func (this *Tx) Commit() error {
	return this.conn.CommitTx(this)
}

func (this *Tx) Rollback() error {
	return this.conn.RollbackTx(this)
}
