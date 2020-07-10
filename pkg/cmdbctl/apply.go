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
	"io"

	ym "github.com/ghodss/yaml"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v2"
)

const (
	KindType         = "cmdb#type"
	KindObject       = "cmdb#object"
	KindRelation     = "cmdb#relation"
	KindRelationType = "cmdb#relation-type"
)

type Any struct {
	Kind string `json:"kind"`
}

func (c *Client) Apply(ctx context.Context, yamlCodes string) error {
	decoder := newDecoder(yamlCodes)
	for {
		var any Any
		jsonBytes, err := decoder.Decode(&any)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		switch any.Kind {
		case KindType:
			var typ v1.ObjectType
			err = convert(jsonBytes, &typ)
			if err != nil {
				return err
			}
			err = c.CreateOrUpdateType(ctx, &typ)
			if err != nil {
				return err
			}
		case KindObject:
			var obj v1.Object
			err = convert(jsonBytes, &obj)
			if err != nil {
				return err
			}
			err = c.CreateOrUpdateObject(ctx, &obj)
			if err != nil {
				return err
			}
		case KindRelation:
			var rel v1.Relation
			err = convert(jsonBytes, &rel)
			if err != nil {
				return err
			}
			err = c.CreateOrUpdateRelation(ctx, &rel)
			if err != nil {
				return err
			}
		case KindRelationType:
			var rel v1.RelationType
			err = convert(jsonBytes, &rel)
			if err != nil {
				return err
			}
			err = c.CreateOrUpdateRelationType(ctx, &rel)
			if err != nil {
				return err
			}
		}
	}
}

func (c *Client) CreateOrUpdateObject(ctx context.Context, obj *v1.Object) error {
	obj = v1.CheckObject(obj)
	origin, err := c.ObjectsClient.Get(ctx, &v1.GetObjectRequest{Type: obj.Type, Name: obj.Name})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			_, err = c.ObjectsClient.Create(ctx, obj)
			if err == nil {
				fmt.Printf("object %s/%s created\n", obj.Type, obj.Name)
			} else {
				fmt.Printf("object %s/%s create failed: %s\n", obj.Type, obj.Name, err)
			}
			return err
		}
		return err
	}
	if origin.Equals(obj) {
		fmt.Printf("object %s/%s unchanged\n", obj.Type, obj.Name)
		return nil
	}
	_, err = c.ObjectsClient.Update(ctx, &v1.ObjectUpdateRequest{Object: obj})
	if err == nil {
		fmt.Printf("object %s/%s updated\n", obj.Type, obj.Name)
	} else {
		fmt.Printf("object %s/%s update failed: %s\n", obj.Type, obj.Name, err)
	}

	return err
}

func (c *Client) CreateOrUpdateType(ctx context.Context, typ *v1.ObjectType) error {
	typ = v1.CheckObjectType(typ)
	origin, err := c.ObjectTypesClient.Get(ctx, &v1.GetObjectTypeRequest{Name: typ.Name})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			_, err = c.ObjectTypesClient.Create(ctx, typ)
			if err == nil {
				fmt.Printf("type %s created\n", typ.Name)
			} else {
				fmt.Printf("type %s create failed: %s\n", typ.Name, err)
			}
			return err
		}
		return err
	}
	if origin.Equals(typ) {
		fmt.Printf("type %s unchanged\n", typ.Name)
		return nil
	}
	_, err = c.ObjectTypesClient.Update(ctx, &v1.UpdateObjectTypeRequest{Type: typ})
	if err == nil {
		fmt.Printf("type %s updated\n", typ.Name)
	} else {
		fmt.Printf("type %s update failed: %s\n", typ.Name, err)
	}

	return err
}

func (c *Client) CreateOrUpdateRelationType(ctx context.Context, rel *v1.RelationType) error {
	rel = v1.CheckRelationType(rel)
	origin, err := c.RelationTypesClient.Get(ctx, &v1.RelationType{Name: rel.Name, From: rel.From, To: rel.To})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			_, err = c.RelationTypesClient.Create(ctx, rel)
			if err == nil {
				fmt.Printf("relation type %s created\n", rel.Name)
			} else {
				fmt.Printf("relation type %s create failed: %s\n", rel.Name, err)
			}
			return err
		}
		return err
	}
	if origin.Equals(rel) {
		fmt.Printf("relation type %s unchanged\n", rel.Name)
		return nil
	}
	_, err = c.RelationTypesClient.Update(ctx, &v1.UpdateRelationTypeRequest{Type: rel})
	if err == nil {
		fmt.Printf("relation type %s updated\n", rel.Name)
	} else {
		fmt.Printf("relation type %s update failed: %s\n", rel.Name, err)
	}
	return err
}

func (c *Client) CreateOrUpdateRelation(ctx context.Context, r *v1.Relation) error {
	r = v1.CheckRelation(r)
	origin, err := c.RelationsClient.Get(ctx, &v1.Relation{
		Relation: r.Relation,
		From:     r.From,
		To:       r.To,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			_, err = c.RelationsClient.Create(ctx, r)
			if err == nil {
				fmt.Printf("relation %s[%s/%s->%s/%s] created\n", r.Relation, r.From.Type, r.From.Name, r.To.Type, r.To.Name)
			} else {
				fmt.Printf("relation %s[%s/%s->%s/%s] create failed: %s\n", r.Relation, r.From.Type, r.From.Name, r.To.Type, r.To.Name, err)
			}
			return err
		}
		return err
	}

	if origin.Equals(r) {
		fmt.Printf("relation %s[%s/%s->%s/%s] unchanged\n", r.Relation, r.From.Type, r.From.Name, r.To.Type, r.To.Name)
		return nil
	}

	_, err = c.RelationsClient.Update(ctx, &v1.UpdateRelationRequest{Relation: r})
	if err == nil {
		fmt.Printf("relation %s[%s/%s->%s/%s] updated\n", r.Relation, r.From.Type, r.From.Name, r.To.Type, r.To.Name)
	} else {
		fmt.Printf("relation %s[%s/%s->%s/%s] update failed: %s\n", r.Relation, r.From.Type, r.From.Name, r.To.Type, r.To.Name, err)
	}
	return err
}

func convert(data []byte, n proto.Message) error {
	dataBuf := bytes.NewBuffer(data)
	err := (&jsonpb.Unmarshaler{AllowUnknownFields: true}).Unmarshal(dataBuf, n)
	if err != nil && err == io.EOF {
		return nil
	}
	return err
}

type Decoder struct {
	decoder *yaml.Decoder
}

func newDecoder(yamlCodes string) *Decoder {
	d := &Decoder{}
	d.decoder = yaml.NewDecoder(bytes.NewBuffer([]byte(yamlCodes)))
	return d
}

func (d *Decoder) Decode(obj interface{}) (jsonBytes []byte, err error) {
	var m = map[string]interface{}{}
	err = d.decoder.Decode(&m)
	if err != nil {
		return nil, err
	}
	temp, _ := yaml.Marshal(m)
	jsonBytes, err = ym.YAMLToJSON(temp)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(jsonBytes, obj)
	return jsonBytes, err
}
