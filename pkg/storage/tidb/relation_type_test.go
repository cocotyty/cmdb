// Copyright 2020 Zhizhesihai (Beijing) Technology Limited.
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

package tidb

import (
	"context"
	"testing"

	v1 "github.com/zhihu/cmdb/pkg/api/v1"
)

func TestStorage_CreateRelationType(t *testing.T) {
	storage := getTestStorage(t)
	var ctx = context.Background()
	_, err := storage.CreateRelationType(ctx, &v1.RelationType{
		Name:        "BELONGS",
		From:        "object_test_resource",
		To:          "object_test_resource",
		Description: "",
		Metas: map[string]*v1.ObjectMeta{
			"size": {
				Name:      "size",
				ValueType: v1.ValueType_DOUBLE,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

}
