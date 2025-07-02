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
	"crypto/sha256"
	"fmt"
	"github.com/google/uuid"
	"regexp"
	"strings"
	"time"
)

func GenerateUniqueId(groupName string) (string, error) {
	if newUuid, err := GenerateUuid(); err != nil {
		return "", err
	} else {
		re1 := regexp.MustCompile("[-T:.Z]")
		re2 := regexp.MustCompile("^(.{7}).*")

		hash := sha256.New()
		hash.Write([]byte(groupName))

		sb := strings.Builder{}
		sb.WriteString(strings.ReplaceAll(newUuid, "-", ""))
		sb.WriteString(re1.ReplaceAllString(time.Now().UTC().Format(time.RFC3339Nano), ""))
		sb.WriteString(re2.ReplaceAllString(fmt.Sprintf("%x", hash.Sum(nil)), "$1"))
		return sb.String(), nil
	}
}

func GenerateUuid() (string, error) {
	if newUuid, err := uuid.NewRandom(); err != nil {
		return "", err
	} else {
		return newUuid.String(), nil
	}
}
