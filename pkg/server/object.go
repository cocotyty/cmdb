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
	"strings"

	"github.com/juju/loggo"
	"github.com/zhihu/cmdb/pkg/api/v1"
	"github.com/zhihu/cmdb/pkg/query"
	"github.com/zhihu/cmdb/pkg/storage"
	"github.com/zhihu/cmdb/pkg/storage/cdc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = loggo.GetLogger("server")

type Objects struct {
	Storage storage.Storage
}

func (o *Objects) Relations(ctx context.Context, request *v1.GetObjectRequest) (*v1.ListRelationResponse, error) {
	relations, err := o.Storage.ListObjectRelations(ctx, &v1.ObjectReference{
		Type: request.Type,
		Name: request.Name,
	})
	if err != nil {
		return nil, err
	}
	return &v1.ListRelationResponse{Relations: relations}, nil
}

func (o *Objects) Delete(ctx context.Context, request *v1.DeleteObjectRequest) (*v1.Object, error) {
	object, err := o.Storage.DeleteObject(ctx, request.Type, request.Name)
	return object, err
}

func (o *Objects) Update(ctx context.Context, request *v1.ObjectUpdateRequest) (*v1.Object, error) {
	if request.Object == nil {
		return nil, status.New(codes.InvalidArgument, "object must not be empty").Err()
	}
	request.Object = v1.CheckObject(request.Object)

	action := storage.ObjectUpdateOption{}
	var updateMetas = map[string]*v1.ObjectMetaValue{}
	var paths []string
	if request.UpdateMask != nil {
		paths = request.UpdateMask.Paths
	}
	if len(paths) == 0 {
		action = storage.ObjectUpdateOption{
			SetStatus:      true,
			SetState:       true,
			SetDescription: true,
			SetAllMeta:     true,
			MatchVersion:   request.MatchVersion,
		}
	}
	for _, path := range paths {
		var names = strings.Split(path, ".")
		if len(names) == 0 {
			continue
		}
		switch names[0] {
		case "metas":
			if len(names) == 1 {
				action.SetAllMeta = true
			} else {
				updateMetas[names[1]] = request.Object.Metas[names[1]]
			}
			continue
		case "status":
			action.SetStatus = true
		case "state":
			action.SetState = true
		case "description":
			action.SetDescription = true
		}
	}
	if !action.SetAllMeta {
		request.Object.Metas = updateMetas
	}
	return o.Storage.UpdateObject(ctx, action, request.Object)
}

func (o *Objects) Get(ctx context.Context, request *v1.GetObjectRequest) (*v1.Object, error) {
	return o.Storage.GetObject(ctx, request.Type, request.Name)
}

func (o *Objects) Watch(request *v1.ListObjectRequest, server v1.Objects_WatchServer) error {
	f := &FilterWatcher{
		server: server,
	}
	if request.Query != "" {
		selector, err := query.Parse(request.Query)
		if err != nil {
			return err
		}
		f.selector = selector
	}
	return o.Storage.WatchObjects(server.Context(), request.Type, f)
}

type FilterWatcher struct {
	server   v1.Objects_WatchServer
	selector storage.Selector
}

func (f *FilterWatcher) OnInit(objects []*v1.Object) {
	_ = f.server.Send(&v1.ObjectEvent{
		Objects: objects,
		Type:    v1.WatchEventType_INIT,
	})
}

func (f *FilterWatcher) Filter(object *v1.Object) bool {
	if f.selector == nil {
		return true
	}
	return f.selector.Match(object.Metas)
}

func (f *FilterWatcher) OnEvent(event storage.ObjectEvent) {
	evt := &v1.ObjectEvent{
		Objects: []*v1.Object{event.Object},
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

func (o *Objects) Create(ctx context.Context, object *v1.Object) (*v1.Object, error) {
	object = v1.CheckObject(object)
	n, err := o.Storage.CreateObject(ctx, object)
	return n, err
}

func (o *Objects) List(ctx context.Context, request *v1.ListObjectRequest) (*v1.ListObjectResponse, error) {
	return o.Storage.ListObjects(ctx, request)
}
