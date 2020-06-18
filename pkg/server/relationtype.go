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

package server

import (
	"context"

	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"github.com/zhihu/cmdb/pkg/storage"
)

type RelationTypes struct {
	Storage storage.Storage
}

func (r *RelationTypes) Create(ctx context.Context, relationType *v1.RelationType) (*v1.RelationType, error) {
	return r.Storage.CreateRelationType(ctx, relationType)
}

func (r *RelationTypes) Update(ctx context.Context, request *v1.RelationTypeUpdateRequest) (*v1.RelationType, error) {
	return r.Storage.UpdateRelationType(ctx, request.Type, request.UpdateMask.Paths)
}

func (r *RelationTypes) List(ctx context.Context, request *v1.ListRelationTypesRequest) (*v1.ListRelationTypesResponse, error) {
	types, err := r.Storage.ListRelationType(ctx, request.Consistent, request.ShowDeleted)
	if err != nil {
		return nil, err
	}
	return &v1.ListRelationTypesResponse{Types: types}, nil
}

func (r *RelationTypes) Delete(ctx context.Context, relationType *v1.RelationType) (*v1.RelationType, error) {
	return r.Storage.DeleteRelationType(ctx, relationType.From, relationType.To, relationType.Name)
}
