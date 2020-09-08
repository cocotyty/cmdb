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

package cmdbctl

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/juju/loggo"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"google.golang.org/grpc"
)

var log = loggo.GetLogger("")

type Client struct {
	conn                *grpc.ClientConn
	ObjectsClient       v1.ObjectsClient
	ObjectTypesClient   v1.ObjectTypesClient
	RelationsClient     v1.RelationsClient
	RelationTypesClient v1.RelationTypesClient
}

func NewClient(ctx context.Context, s string) (c *Client, err error) {
	c = &Client{}
	conn, err := grpc.DialContext(ctx, s, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Errorf("did not connect: %v", err)
		return nil, err
	}
	c.conn = conn
	c.ObjectsClient = v1.NewObjectsClient(conn)
	c.ObjectTypesClient = v1.NewObjectTypesClient(conn)
	c.RelationsClient = v1.NewRelationsClient(conn)
	c.RelationTypesClient = v1.NewRelationTypesClient(conn)
	return c, nil
}

func (c *Client) Watch(ctx context.Context, name string) error {
	req := &v1.ListObjectRequest{}
	req.Type = name
	w, err := c.ObjectsClient.Watch(ctx, req)
	if err != nil {
		return err
	}
	for {
		event, err := w.Recv()
		if err != nil {
			return err
		}
		json := &runtime.JSONPb{OrigName: true, EmitDefaults: true}
		msg, err := json.Marshal(event)
		fmt.Printf("event: %s %v \n", msg, err)
	}
}
