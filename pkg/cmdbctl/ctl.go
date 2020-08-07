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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/golang/protobuf/jsonpb"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/juju/loggo"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
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

func (c *Client) Get(ctx context.Context, name string, query string) error {
	req := &v1.ListObjectRequest{}
	req.Type = name
	req.Query = query
	req.View = v1.ObjectView_NORMAL
	resp, err := c.ObjectsClient.List(ctx, req)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(w, "NAME\tSTATUS\tSTATE\tMETAS\tDESC\n")
	for _, object := range resp.Objects {
		buf := bytes.NewBuffer(nil)
		for name, value := range object.Metas {
			if buf.Len() > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(name)
			buf.WriteString(": ")
			buf.WriteString(value.Value)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", object.Name, object.Status, object.State, buf.String(), object.Description)
	}
	w.Flush()
	return err
}

const (
	StandardFormat = "standard"
	JSON           = "json"
	YAML           = "yaml"
)

type Format struct {
	Type string
	Arg  string
}

func (c *Client) GetType(ctx context.Context, f *Format) error {
	req := &v1.ListObjectTypesRequest{}
	req.Consistent = true
	resp, err := c.ObjectTypesClient.List(ctx, req)
	if err != nil {
		return err
	}
	switch f.Type {
	case YAML:
		m := &jsonpb.Marshaler{
			OrigName:     false,
			EnumsAsInts:  false,
			EmitDefaults: true,
			Indent:       "  ",
			AnyResolver:  nil,
		}
		var list = List{Items: []interface{}{}, Kind: "list"}
		for _, objectType := range resp.Types {
			var object = map[string]interface{}{}
			data, _ := m.MarshalToString(objectType)
			json.Unmarshal([]byte(data), &object)
			list.Items = append(list.Items, object)
		}
		encoder := yaml.NewEncoder(os.Stdout)
		encoder.Encode(list.Items)
		return nil
	case JSON:
		m := &jsonpb.Marshaler{
			OrigName:     false,
			EnumsAsInts:  false,
			EmitDefaults: true,
			Indent:       "  ",
			AnyResolver:  nil,
		}
		var list = List{Items: []interface{}{}, Kind: "list"}
		for _, objectType := range resp.Types {
			var object = map[string]interface{}{}
			data, _ := m.MarshalToString(objectType)
			_ = json.Unmarshal([]byte(data), &object)
			list.Items = append(list.Items, object)
		}
		data, _ := json.MarshalIndent(list, "", "  ")
		_, _ = os.Stdout.Write(data)
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	_, _ = fmt.Fprintf(w, "NAME\tMETAS\tSTATUSES\tDESC\n")
	for _, t := range resp.Types {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", t.Name, formatMetas(t.Metas), formatStatuses(t.Statuses), maxStr(t.Description, 10))
	}
	_ = w.Flush()
	return err
}

func formatMetas(metas map[string]*v1.ObjectMeta) string {
	var list []*v1.ObjectMeta
	for _, meta := range metas {
		list = append(list, meta)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	var names []string
	for _, meta := range list {
		names = append(names, meta.Name+"("+meta.ValueType.String()+")")
	}
	return strings.Join(names, " | ")
}

func formatStatuses(statuses map[string]*v1.ObjectStatus) string {
	var list []*v1.ObjectStatus
	for _, status := range statuses {
		list = append(list, status)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	var statusStrs []string
	for _, status := range list {
		var names []string
		for name := range status.States {
			names = append(names, name)
		}
		sort.Strings(names)
		statusStrs = append(statusStrs, status.Name+"["+strings.Join(names, ",")+"]")
	}
	return strings.Join(statusStrs, " | ")
}

func maxStr(str string, max int) string {
	if len(str) > max {
		return str[:max-2] + ".."
	}
	return str
}

type List struct {
	Items []interface{} `json:"items"`
	Kind  string        `json:"kind"`
}
