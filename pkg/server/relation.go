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
	"github.com/zhihu/cmdb/pkg/query"
	"github.com/zhihu/cmdb/pkg/storage"
	"github.com/zhihu/cmdb/pkg/storage/cdc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Relations struct {
	Storage storage.Storage
}

func (r *Relations) Edges(ctx context.Context, relation *v1.Relation) (*v1.ListRelationResponse, error) {
	if relation.From == nil ||
		relation.To == nil ||
		relation.From.Type == "" ||
		relation.To.Type == "" || relation.Relation == "" {
		return nil, status.New(codes.InvalidArgument, "relation's type must not empty").Err()
	}
	if relation.From.Name == "_" {
		relation.From.Name = ""
	}

	if relation.To.Name == "_" {
		relation.To.Name = ""
	}

	relations, err := r.Storage.FindRelations(ctx, relation)
	if err != nil {
		return nil, err
	}
	return &v1.ListRelationResponse{Relations: relations}, nil
}

func (r *Relations) Watch(request *v1.WatchRelationRequest, server v1.Relations_WatchServer) error {
	ctx := server.Context()
	selector, err := query.Parse(request.Query)
	if err != nil {
		return status.New(codes.InvalidArgument, "query format invalid").Err()
	}

	w := &RelationFilterWatcher{
		server:   server,
		selector: selector,
	}
	return r.Storage.WatchRelation(ctx, request.From, request.To, request.Relation, w)
}

func (r *Relations) Create(ctx context.Context, relation *v1.Relation) (*v1.Relation, error) {
	return r.Storage.CreateRelation(ctx, relation)
}

func (r *Relations) check(relation *v1.Relation) (checked *v1.Relation, err error) {
	relation = v1.CheckRelation(relation)
	if relation.From == nil ||
		relation.From.Type == "" ||
		relation.From.Name == "" {
		return nil, status.New(codes.InvalidArgument, "from must not be empty").Err()
	}
	if relation.To == nil ||
		relation.To.Type == "" ||
		relation.To.Name == "" {
		return nil, status.New(codes.InvalidArgument, "to must not be empty").Err()
	}
	if relation.Relation == "" {
		return nil, status.New(codes.InvalidArgument, "relation must not be empty").Err()
	}
	return relation, nil
}

func (r *Relations) Get(ctx context.Context, relation *v1.Relation) (*v1.Relation, error) {
	relation, err := r.check(relation)
	if err != nil {
		return nil, err
	}
	return r.Storage.GetRelation(ctx, relation.From, relation.To, relation.Relation)
}

func (r *Relations) Update(ctx context.Context, request *v1.UpdateRelationRequest) (*v1.Relation, error) {
	if request.Relation == nil {
		return nil, status.New(codes.InvalidArgument, "relation must not be empty").Err()
	}
	var err error
	request.Relation, err = r.check(request.Relation)
	if err != nil {
		return nil, err
	}
	var paths []string
	if request.UpdateMask != nil && request.UpdateMask.Paths != nil {
		paths = request.UpdateMask.Paths
	}
	return r.Storage.UpdateRelation(ctx, request.Relation, paths)
}

func (r *Relations) Delete(ctx context.Context, relation *v1.Relation) (*v1.Relation, error) {
	relation, err := r.check(relation)
	if err != nil {
		return nil, err
	}
	return r.Storage.DeleteRelation(ctx, relation)
}

func (r *Relations) List(ctx context.Context, request *v1.ListRelationRequest) (*v1.ListRelationResponse, error) {
	if request.From == "" || request.To == "" {
		return nil, status.New(codes.InvalidArgument, "`from` or `to` must not be empty").Err()
	}
	relations, err := r.Storage.ListRelations(ctx, request.From, request.To, request.Relation)
	if err != nil {
		return nil, err
	}
	return &v1.ListRelationResponse{Relations: relations}, nil
}

type RelationFilterWatcher struct {
	server   v1.Relations_WatchServer
	selector storage.Selector
}

func (f *RelationFilterWatcher) OnInit(relations []*v1.RichRelation) {
	_ = f.server.Send(&v1.RelationEvent{
		Relations: relations,
		Type:      v1.WatchEventType_INIT,
	})
}

func (f *RelationFilterWatcher) Filter(relations *v1.RichRelation) bool {
	if f.selector == nil {
		return true
	}
	return f.selector.Match(relations.Metas)
}

func (f *RelationFilterWatcher) OnEvent(event storage.RelationEvent) {
	evt := &v1.RelationEvent{
		Relations: []*v1.RichRelation{event.Relation},
	}
	switch event.Event {
	case cdc.Create:
		evt.Type = v1.WatchEventType_CREATE
	case cdc.Update:
		evt.Type = v1.WatchEventType_UPDATE
	case cdc.Delete:
		evt.Type = v1.WatchEventType_DELETE
	}
	_ = f.server.Send(evt)
}
