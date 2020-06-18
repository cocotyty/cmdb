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

type Relation struct {
	Storage storage.Storage
}

func (r *Relation) Create(ctx context.Context, relation *v1.Relation) (*v1.Relation, error) {
	return r.Storage.CreateRelation(ctx, relation)
}

func (r *Relation) Get(ctx context.Context, relation *v1.Relation) (*v1.Relation, error) {
	return r.Storage.GetRelation(ctx, relation.From, relation.To, relation.Relation)
}

func (r *Relation) Update(ctx context.Context, request *v1.UpdateRelationRequest) (*v1.Relation, error) {
	return r.Storage.UpdateRelation(ctx, request.Relation, request.UpdateMask.Paths)
}

func (r *Relation) Delete(ctx context.Context, relation *v1.Relation) (*v1.Relation, error) {
	return r.Storage.DeleteRelation(ctx, relation)
}

func (r *Relation) List(ctx context.Context, request *v1.ListRelationRequest) (*v1.ListRelationResponse, error) {
	relations, err := r.Storage.ListRelations(ctx, request.From, request.To, request.Relation)
	if err != nil {
		return nil, err
	}
	return &v1.ListRelationResponse{Relations: relations}, nil
}
