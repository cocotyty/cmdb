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
	"database/sql"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"github.com/zhihu/cmdb/pkg/model"
	"github.com/zhihu/cmdb/pkg/model/typetables"
	"github.com/zhihu/cmdb/pkg/tools/sqly"
)

type relationTypeMetas struct {
	byID   map[int]*model.ObjectRelationMeta
	byName map[string]*model.ObjectRelationMeta
}

func (s *Storage) loadRelationTypes(fromType string, toType string, relation string) (metas relationTypeMetas,
	relationTypeID int,
	fromTypeID int,
	toTypeID int) {
	metas.byID = map[int]*model.ObjectRelationMeta{}
	metas.byName = map[string]*model.ObjectRelationMeta{}

	s.cache.TypeCache(func(d *typetables.Database) {
		fromType, ok := d.ObjectTypeTable.GetByName(fromType)
		if !ok {
			return
		}
		toType, ok := d.ObjectTypeTable.GetByName(toType)
		if !ok {
			return
		}
		typ, ok := d.ObjectRelationTypeTable.GetByLogicalID(fromType.ID, toType.ID, relation)
		if !ok {
			return
		}
		fromTypeID = typ.FromTypeID
		toTypeID = typ.ToTypeID
		relationTypeID = typ.ID
		for _, meta := range typ.ObjectRelationMeta {
			copied := meta.ObjectRelationMeta
			metas.byName[meta.Name] = &copied
			metas.byID[meta.ID] = &copied
		}
	})
	return metas, relationTypeID, fromTypeID, toTypeID
}

func (s *Storage) CreateRelation(ctx context.Context, relation *v1.Relation) (created *v1.Relation, err error) {
	metas, relationTypeID, fromTypeID, toTypeID := s.loadRelationTypes(relation.From.Type, relation.To.Type, relation.Relation)
	if relationTypeID == 0 {
		return nil, notFound("no such relation type: %s(%s=>%s)", relation.Relation, relation.From.Type, relation.To.Type)
	}
	ts, err := s.GetTS(ctx)
	if err != nil {
		return nil, internalError(err)
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()
	var fromObject = &model.Object{}
	var toObject = &model.Object{}
	var objects []*model.Object

	err = tx.SelectContext(ctx, &objects,
		"select  * from object where ( type_id = ? and name = ?) or ( type_id=? and name = ?) for update",
		fromTypeID, relation.From.Name, ts, toTypeID, relation.To.Name, ts,
	)
	if len(objects) < 2 {
		return nil, notFound("object not found")
	}

	for _, object := range objects {
		if object.RelationVersion > ts {
			return nil, aborted("operation conflict")
		}
		if object.Name == relation.From.Name && object.TypeID == fromTypeID {
			fromObject = object
		}
		if object.Name == relation.To.Name && object.TypeID == toTypeID {
			toObject = object
		}
	}
	if fromObject.ID == 0 || toObject.ID == 0 {
		return nil, notFound("object not found")
	}
	now := time.Now()
	e := &sqly.Execer{
		Ctx: ctx,
		Tx:  tx,
	}
	e.Exec("insert into object_relation (from_object_id, relation_type_id, to_object_id, create_time) VALUES (?,?,?,?)",
		fromObject.ID, relationTypeID, toObject.ID, now,
	)

	e.Exec("update object set relation_version = ? where id in (?,?)", ts, fromObject.ID, toObject.ID)

	for name, value := range relation.Metas {
		metaId, ok := metas.byName[name]
		if !ok {
			continue
		}
		e.Exec("insert into object_relation_meta_value (from_object_id, relation_type_id, to_object_id, meta_id, value, create_time) VALUES (?,?,?,?,?,?)",
			fromObject.ID, relationTypeID, toObject.ID, metaId, value.Value, now,
		)
	}
	if e.Err != nil {
		return nil, internalError(err)
	}
	relation.CreateTime, _ = ptypes.TimestampProto(now)
	relation.UpdateTime = nil
	relation.DeleteTime = nil
	return relation, nil
}

func (s *Storage) GetRelation(ctx context.Context, from *v1.ObjectReference, to *v1.ObjectReference, relationType string, showDeleted bool) (rel *v1.Relation, err error) {
	metas, relationTypeID, fromTypeID, toTypeID := s.loadRelationTypes(from.Type, to.Type, relationType)

	if relationTypeID == 0 {
		return nil, notFound("no such relation type: %s(%s=>%s)", relationType, from.Type, to.Type)
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}

	relation, err := s.getRelationByID(ctx, tx, relationTypeID, fromTypeID, toTypeID, showDeleted)
	if err != nil {
		return nil, err
	}
	relMetas, err := s.getRelationMetasByID(ctx, tx, relationTypeID, fromTypeID, toTypeID)
	if err != nil {
		return nil, err
	}
	rel = &v1.Relation{
		Relation: relationType,
		From:     from,
		To:       to,
		Metas:    make(map[string]*v1.ObjectMetaValue, len(relMetas)),
	}
	rel.CreateTime, _ = ptypes.TimestampProto(relation.CreateTime)
	if relation.UpdateTime != nil {
		rel.UpdateTime, _ = ptypes.TimestampProto(*relation.UpdateTime)
	}
	if relation.DeleteTime != nil {
		rel.DeleteTime, _ = ptypes.TimestampProto(*relation.DeleteTime)
	}
	for _, meta := range relMetas {
		if meta.DeleteTime != nil {
			continue
		}
		m, ok := metas.byID[meta.MetaID]
		if !ok {
			continue
		}
		rel.Metas[m.Name] = &v1.ObjectMetaValue{
			ValueType: v1.ValueType(m.ValueType),
			Value:     meta.Value,
		}
	}
	return rel, nil
}

func (s *Storage) getRelationByID(ctx context.Context, tx *sqlx.Tx, relationTypeID, fromObjectID, toObjectID int, showDeleted bool) (*model.ObjectRelation, error) {
	var relation model.ObjectRelation
	err := tx.GetContext(ctx, &relation, "select * from object_relation where relation_type_id = ? and from_object_id = ? and to_object_id = ? limit 1",
		relationTypeID, fromObjectID, toObjectID,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, notFound("relation not found")
		}
		return nil, internalError(err)
	}
	if !showDeleted && relation.DeleteTime != nil {
		return nil, notFound("relation not found")
	}
	return &relation, nil
}

func (s *Storage) getRelationMetasByID(ctx context.Context, tx *sqlx.Tx, relationTypeID, fromObjectID, toObjectID int) ([]*model.ObjectRelationMetaValue, error) {
	var values []*model.ObjectRelationMetaValue
	err := tx.GetContext(ctx, &values, "select * from object_relation_meta_value where relation_type_id = ? and from_object_id = ? and to_object_id = ?",
		relationTypeID, fromObjectID, toObjectID,
	)
	if err != nil {
		return nil, internalError(err)
	}
	return values, nil
}
