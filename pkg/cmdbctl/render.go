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
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"gopkg.in/yaml.v2"
)

const (
	StandardFormat = "standard"
	JSON           = "json"
	YAML           = "yaml"
)

type Format struct {
	Type string
	Arg  string
}

type List struct {
	Items []interface{} `json:"items"`
	Kind  string        `json:"kind"`
}

func RenderYAML(i interface{}, omitList bool) error {
	m := &jsonpb.Marshaler{
		OrigName:     false,
		EnumsAsInts:  false,
		EmitDefaults: true,
		Indent:       "  ",
		AnyResolver:  nil,
	}
	var list = List{Items: []interface{}{}, Kind: "list"}
	arr := reflect.ValueOf(i)
	size := arr.Len()
	for i := 0; i < size; i++ {
		item := arr.Index(i)
		var object = map[string]interface{}{}
		data, _ := m.MarshalToString(item.Interface().(proto.Message))
		json.Unmarshal([]byte(data), &object)

		var kind string
		switch item.Interface().(type) {
		case *v1.ObjectType:
			kind = KindType
		case *v1.Object:
			kind = KindObject
		case *v1.Relation:
			kind = KindRelation
		case *v1.RelationType:
			kind = KindRelationType
		}
		object["kind"] = kind

		list.Items = append(list.Items, object)
	}
	encoder := yaml.NewEncoder(os.Stdout)
	if len(list.Items) == 1 && omitList {
		_ = encoder.Encode(list.Items[0])
	} else {
		_ = encoder.Encode(list.Items)
	}
	return nil
}

func RenderJSON(i interface{}, omitList bool) error {
	m := &jsonpb.Marshaler{
		OrigName:     false,
		EnumsAsInts:  false,
		EmitDefaults: true,
		Indent:       "  ",
		AnyResolver:  nil,
	}
	arr := reflect.ValueOf(i)
	size := arr.Len()
	var list = List{Items: []interface{}{}, Kind: "list"}
	for i := 0; i < size; i++ {
		item := arr.Index(i)
		var object = map[string]interface{}{}
		data, _ := m.MarshalToString(item.Interface().(proto.Message))
		_ = json.Unmarshal([]byte(data), &object)
		var kind string
		switch item.Interface().(type) {
		case *v1.ObjectType:
			kind = KindType
		case *v1.Object:
			kind = KindObject
		case *v1.Relation:
			kind = KindRelation
		case *v1.RelationType:
			kind = KindRelationType
		}
		object["kind"] = kind

		list.Items = append(list.Items, object)
	}
	var data []byte
	if len(list.Items) == 1 && omitList {
		data, _ = json.MarshalIndent(list.Items[0], "", "  ")
	} else {
		data, _ = json.MarshalIndent(list, "", "  ")
	}
	_, _ = os.Stdout.Write(data)
	return nil
}

func formatMetaValues(metas map[string]*v1.ObjectMetaValue) string {
	var names []string
	for name, meta := range metas {
		names = append(names, name+":"+meta.Value)
	}
	return strings.Join(names, ",")
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
