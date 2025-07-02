package database

import "database/sql/driver"

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

// Reference: https://github.com/CanonicalLtd/go-grpc-sql/blob/181d263025fb02a680c1726752eb27f3a2154e26/result.go

var _ driver.Result = (*Result)(nil)

type Result struct {
	rowsAffected int32
}

func (this *Result) LastInsertId() (int64, error) {
	return -1, nil
}

func (this *Result) RowsAffected() (int64, error) {
	return int64(this.rowsAffected), nil
}
