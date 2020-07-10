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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RelationTypes struct {
	Storage storage.Storage
}

func (r *RelationTypes) Get(ctx context.Context, relationType *v1.RelationType) (*v1.RelationType, error) {
	if relationType == nil {
		return nil, status.New(codes.InvalidArgument, "parameter must not be empty").Err()
	}
	relationType = v1.CheckRelationType(relationType)
	return r.Storage.GetRelationType(ctx, relationType.Name, relationType.From, relationType.To)
}

func (r *RelationTypes) Create(ctx context.Context, relationType *v1.RelationType) (*v1.RelationType, error) {
	if relationType == nil {
		return nil, status.New(codes.InvalidArgument, "parameter must not be empty").Err()
	}
	relationType = v1.CheckRelationType(relationType)
	return r.Storage.CreateRelationType(ctx, relationType)
}

func (r *RelationTypes) Update(ctx context.Context, request *v1.UpdateRelationTypeRequest) (*v1.RelationType, error) {
	if request.Type == nil {
		return nil, status.New(codes.InvalidArgument, "type must not be empty").Err()
	}
	request.Type = v1.CheckRelationType(request.Type)
	var paths []string
	if request.UpdateMask != nil {
		paths = request.UpdateMask.Paths
	}
	return r.Storage.UpdateRelationType(ctx, request.Type, paths)
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
