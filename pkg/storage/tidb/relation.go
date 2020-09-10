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
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"github.com/zhihu/cmdb/pkg/model"
	"github.com/zhihu/cmdb/pkg/model/typetables"
	"github.com/zhihu/cmdb/pkg/storage"
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

func (s *Storage) getRelationObjects(ctx context.Context, tx *sqlx.Tx, ts uint64, fromTypeID int, toTypeID int, fromName string, toName string) (fromObject, toObject *model.Object, err error) {
	fromObject = &model.Object{}
	toObject = &model.Object{}
	var objects []*model.Object

	err = tx.SelectContext(ctx, &objects,
		"select  * from object where ( type_id = ? and name = ?) or ( type_id=? and name = ?) for update",
		fromTypeID, fromName, toTypeID, toName,
	)
	if len(objects) < 2 {
		return nil, nil, notFound("object not found")
	}

	for _, object := range objects {
		if ts != 0 {
			if object.RelationVersion > ts {
				return nil, nil, aborted("operation conflict")
			}
		}
		if object.Name == fromName && object.TypeID == fromTypeID {
			fromObject = object
		}
		if object.Name == toName && object.TypeID == toTypeID {
			toObject = object
		}
	}
	if fromObject.ID == 0 || toObject.ID == 0 {
		return nil, nil, notFound("object not found")
	}
	return fromObject, toObject, nil
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
	fromObject, toObject, err := s.getRelationObjects(ctx, tx, ts, fromTypeID, toTypeID, relation.From.Name, relation.To.Name)
	if err != nil {
		return nil, err
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
		m, ok := metas.byName[name]
		if !ok {
			continue
		}
		e.Exec("insert into object_relation_meta_value (from_object_id, relation_type_id, to_object_id, meta_id, value, create_time) VALUES (?,?,?,?,?,?)",
			fromObject.ID, relationTypeID, toObject.ID, m.ID, value.Value, now,
		)
	}
	if e.Err != nil {
		return nil, internalError(err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, internalError(err)
	}

	relation.CreateTime, _ = ptypes.TimestampProto(now)
	relation.UpdateTime = nil
	relation.DeleteTime = nil
	return relation, nil
}

func (s *Storage) GetRelation(ctx context.Context, from *v1.ObjectReference, to *v1.ObjectReference, relationType string) (rel *v1.Relation, err error) {
	metas, relationTypeID, fromTypeID, toTypeID := s.loadRelationTypes(from.Type, to.Type, relationType)

	if relationTypeID == 0 {
		return nil, notFound("no such relation type: %s(%s=>%s)", relationType, from.Type, to.Type)
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()
	rel, err = s.getRelation(ctx, tx, relationType, from, to, relationTypeID, fromTypeID, toTypeID, metas)
	return rel, err
}

func (s *Storage) getRelationByID(ctx context.Context, tx *sqlx.Tx, relationTypeID, fromObjectID, toObjectID int) (*model.ObjectRelation, error) {
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
	if relation.DeleteTime != nil {
		return nil, notFound("relation not found")
	}
	return &relation, nil
}

func (s *Storage) getRelationMetasByID(ctx context.Context, tx *sqlx.Tx, relationTypeID, fromObjectID, toObjectID int) ([]*model.ObjectRelationMetaValue, error) {
	var values []*model.ObjectRelationMetaValue
	err := tx.SelectContext(ctx, &values, "select * from object_relation_meta_value where relation_type_id = ? and from_object_id = ? and to_object_id = ?",
		relationTypeID, fromObjectID, toObjectID,
	)
	if err != nil {
		return nil, internalError(err)
	}
	return values, nil
}

func (s *Storage) FindRelations(ctx context.Context, relation *v1.Relation) (relations []*v1.Relation, err error) {
	if relation.From.Name == "" && relation.To.Name == "" {
		return s.ListRelations(ctx, relation.From.Type, relation.To.Type, relation.Relation)
	}
	if relation.From.Name != "" && relation.To.Name != "" {
		rel, err := s.GetRelation(ctx, relation.From, relation.To, relation.Relation)
		if err != nil {
			return nil, err
		}
		return []*v1.Relation{rel}, nil
	}

	var metas = map[int]*model.ObjectRelationMeta{}
	var relType model.ObjectRelationType
	s.cache.TypeCache(func(d *typetables.Database) {
		fromType, ok := d.ObjectTypeTable.GetByName(relation.From.Type)
		if !ok {
			err = notFound("from type not found: %s", relation.From.Type)
			return
		}
		toType, ok := d.ObjectTypeTable.GetByName(relation.To.Type)
		if !ok {
			err = notFound("to type not found: %s", relation.To.Type)
			return
		}

		rrelType, ok := d.ObjectRelationTypeTable.GetByLogicalID(fromType.ID, toType.ID, relation.Relation)
		if !ok {
			err = notFound("relation type not found: %s(%s=>%s)", relation.Relation, relation.From.Type, relation.To.Type)
			return
		}
		for _, meta := range rrelType.ObjectRelationMeta {
			metas[meta.ID] = &meta.ObjectRelationMeta
		}
		relType = rrelType.ObjectRelationType
	})
	if err != nil {
		return nil, err
	}
	var direction string
	var name string
	var typID int
	if relation.From.Name != "" {
		direction = FromDirection
		name = relation.From.Name
		typID = relType.FromTypeID
	} else {
		direction = ToDirection
		name = relation.To.Name
		typID = relType.ToTypeID
	}

	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()
	object, _, err := s.getObject(ctx, tx, typID, name)
	if err != nil {
		return nil, err
	}
	return s.getObjectRelations(ctx, tx, &relType, metas, relation.From.Type, relation.To.Type, direction, object.ID, name)
}

func (s *Storage) ListObjectRelations(ctx context.Context, from *v1.ObjectReference) (relations []*v1.Relation, err error) {
	var metas map[int]*model.ObjectRelationMeta
	var relationTypes map[int]*model.ObjectRelationType
	var objectTypes map[int]string
	var fromTypeID int
	s.cache.TypeCache(func(d *typetables.Database) {
		fromType, ok := d.ObjectTypeTable.GetByName(from.Type)
		if !ok {
			return
		}
		fromTypeID = fromType.ID
		relations := d.ObjectRelationTypeTable.FilterByFromTypeID(fromType.ID)
		for _, relation := range relations {
			fromType, ok := d.ObjectTypeTable.GetByID(relation.FromTypeID)
			if !ok {
				continue
			}
			objectTypes[fromType.ID] = fromType.ObjectType.Name

			toType, ok := d.ObjectTypeTable.GetByID(relation.ToTypeID)
			if !ok {
				continue
			}
			objectTypes[toType.ID] = toType.ObjectType.Name

			var copied = relation.ObjectRelationType
			relationTypes[relation.ID] = &copied
			for _, meta := range relation.ObjectRelationMeta {
				var copied = meta.ObjectRelationMeta
				metas[meta.ID] = &copied
			}

		}
	})
	if fromTypeID == 0 {
		return nil, notFound("no such object: %s/%s", from.Type, from.Name)
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()

	object := &model.Object{}
	err = tx.GetContext(ctx, object, "select * from object where type_id = ? and name = ? limit 1", fromTypeID, from.Name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, notFound("no such object: %s/%s", from.Type, from.Name)
		}
		return nil, internalError(err)
	}

	for _, relationType := range relationTypes {
		objectRelations, err := s.getObjectRelations(ctx, tx, relationType,
			metas,
			from.Type,
			objectTypes[relationType.ToTypeID], FromDirection, object.ID, object.Name)
		if err != nil {
			return nil, err
		}
		relations = append(relations, objectRelations...)
	}
	return relations, nil
}

const (
	FromDirection = "from"
	ToDirection   = "to"
)

func (s *Storage) getObjectRelations(ctx context.Context, tx *sqlx.Tx, relationType *model.ObjectRelationType, metas map[int]*model.ObjectRelationMeta, from, to string, direction string, id int, name string) (relations []*v1.Relation, err error) {
	// find all relation related to this object:
	var objectRelations []model.ObjectRelation
	err = tx.SelectContext(ctx, &objectRelations, "select * from object_relation where relation_type_id = ? and "+direction+"_object_id = ? and delete_time is null",
		relationType.ID, id,
	)
	if err != nil {
		return nil, internalError(err)
	}
	// load all relation's meta values
	var metaValues []*model.ObjectRelationMetaValue
	err = tx.SelectContext(ctx, &metaValues, "select * from object_relation_meta_value where "+direction+"_object_id = ? and delete_time is null", id)
	if err != nil {
		return nil, internalError(err)
	}
	var relationMetaValues = map[int][]*model.ObjectRelationMetaValue{}
	for _, value := range metaValues {
		relationMetaValues[value.ToObjectID] = append(relationMetaValues[value.ToObjectID], value)
	}
	//
	var ids []string
	for _, relation := range objectRelations {
		switch direction {
		case FromDirection:
			ids = append(ids, strconv.Itoa(relation.ToObjectID))
		case ToDirection:
			ids = append(ids, strconv.Itoa(relation.FromObjectID))
		}
	}
	// load all relation object names
	var objectsNames []IDName
	err = tx.SelectContext(ctx, &objectsNames, "select id,name from object where id in ("+strings.Join(ids, ",")+")")
	var objects = map[int]string{}
	for _, idName := range objectsNames {
		objects[idName.ID] = idName.Name
	}

	for _, relation := range objectRelations {
		var rel = &v1.Relation{
			Relation: relationType.Name,
			Metas:    map[string]*v1.ObjectMetaValue{},
		}
		switch direction {
		case FromDirection:
			rel.From = &v1.ObjectReference{
				Type: from,
				Name: name,
			}
			rel.To = &v1.ObjectReference{
				Type: to,
				Name: objects[relation.ToObjectID],
			}
		case ToDirection:
			rel.To = &v1.ObjectReference{
				Type: to,
				Name: name,
			}
			rel.From = &v1.ObjectReference{
				Type: from,
				Name: objects[relation.FromObjectID],
			}
		}
		rel.CreateTime, _ = ptypes.TimestampProto(relation.CreateTime)
		if relation.UpdateTime != nil {
			rel.UpdateTime, _ = ptypes.TimestampProto(*relation.UpdateTime)
		}
		if relation.DeleteTime != nil {
			rel.DeleteTime, _ = ptypes.TimestampProto(*relation.DeleteTime)
		}
		values := relationMetaValues[relation.ToObjectID]
		for _, value := range values {
			rel.Metas[metas[value.MetaID].Name] = &v1.ObjectMetaValue{Value: value.Value, ValueType: v1.ValueType(metas[value.MetaID].ValueType)}
		}
		relations = append(relations, rel)
	}
	return relations, nil
}

func (s *Storage) ListRelations(ctx context.Context, from string, to string, relation string) (rels []*v1.Relation, err error) {
	var metas map[int]*model.ObjectRelationMeta
	var fromTypeID, toTypeID int
	var relationTypeID int
	s.cache.TypeCache(func(d *typetables.Database) {
		fromType, ok := d.ObjectTypeTable.GetByName(from)
		if !ok {
			return
		}
		toType, ok := d.ObjectTypeTable.GetByName(to)
		if !ok {
			return
		}
		fromTypeID = fromType.ID
		toTypeID = toType.ID
		relation, ok := d.ObjectRelationTypeTable.GetByLogicalID(fromTypeID, toTypeID, relation)
		if !ok {
			return
		}
		relationTypeID = relation.ID
		for _, meta := range relation.ObjectRelationMeta {
			var copied = meta.ObjectRelationMeta
			metas[meta.ID] = &copied
		}
	})
	if relationTypeID == 0 {
		return nil, notFound("no such object type: %s", from)
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()
	var relations []*model.ObjectRelation
	err = tx.SelectContext(ctx, &relations, "select * from object_relation where relation_type_id = ?  and delete_time is null", relationTypeID)
	if err != nil {
		return nil, internalError(err)
	}
	if len(relations) == 0 {
		return []*v1.Relation{}, nil
	}

	var objectIDs = make([]string, 0, len(relations)*2)
	for _, objectRelation := range relations {
		objectIDs = append(objectIDs, strconv.Itoa(objectRelation.FromObjectID), strconv.Itoa(objectRelation.ToObjectID))
	}
	var objects []IDName
	queryObjectsNames := "select id,name from object where id "
	queryObjectsNames += " in (" + strings.Join(objectIDs, ",") + ")"
	err = tx.SelectContext(ctx, &objects, queryObjectsNames)
	if err != nil {
		return nil, internalError(err)
	}
	var objectsNames = map[int]string{}
	for _, object := range objects {
		objectsNames[object.ID] = object.Name
	}

	var metaValues []*model.ObjectRelationMetaValue
	err = tx.SelectContext(ctx, &metaValues, "select * from object_relation_meta_value where relation_type_id = ? and delete_time is null ", relationTypeID)
	if err != nil {
		return nil, internalError(err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, internalError(err)
	}

	type ft [2]int
	var values = map[ft][]*model.ObjectRelationMetaValue{}
	for _, value := range metaValues {
		key := ft{value.FromObjectID, value.ToObjectID}
		values[key] = append(values[key], value)
	}
	for _, objectRelation := range relations {
		var rel = &v1.Relation{
			Relation: relation,
			From:     &v1.ObjectReference{Type: from, Name: objectsNames[objectRelation.FromObjectID]},
			To:       &v1.ObjectReference{Type: to, Name: objectsNames[objectRelation.ToObjectID]},
			Metas:    map[string]*v1.ObjectMetaValue{},
		}
		rel.CreateTime, _ = ptypes.TimestampProto(objectRelation.CreateTime)
		if objectRelation.UpdateTime != nil {
			rel.UpdateTime, _ = ptypes.TimestampProto(*objectRelation.UpdateTime)
		}
		if objectRelation.DeleteTime != nil {
			rel.DeleteTime, _ = ptypes.TimestampProto(*objectRelation.DeleteTime)
		}
		relationMetaValues := values[ft{objectRelation.FromObjectID, objectRelation.ToObjectID}]
		for _, value := range relationMetaValues {
			var meta = metas[value.MetaID]
			if meta == nil {
				continue
			}
			rel.Metas[meta.Name] = &v1.ObjectMetaValue{ValueType: v1.ValueType(meta.ValueType), Value: value.Value}
		}
		rels = append(rels, rel)
	}
	return rels, nil
}

func (s *Storage) UpdateRelation(ctx context.Context, relation *v1.Relation, paths []string) (updated *v1.Relation, err error) {
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
	fromObject, toObject, err := s.getRelationObjects(ctx, tx, ts, fromTypeID, toTypeID, relation.From.Name, relation.To.Name)
	if err != nil {
		return nil, err
	}
	e := &sqly.Execer{
		Ctx: ctx,
		Tx:  tx,
	}
	if len(paths) == 0 {
		paths = []string{"metas"}
	}
	for _, path := range paths {
		fields := strings.Split(path, ".")
		switch fields[0] {
		case "metas":
			switch len(fields) {
			case 1:
				// update all metas
				var origin []*model.ObjectRelationMetaValue
				err = tx.SelectContext(ctx, &origin, "select * from object_relation_meta_value where relation_type_id = ? and from_object_id = ? and to_object_id = ?",
					relationTypeID, fromObject.ID, toObject.ID)
				var nameOrigin = map[string]*model.ObjectRelationMetaValue{}
				for _, value := range origin {
					meta, ok := metas.byID[value.MetaID]
					if !ok {
						continue
					}
					nameOrigin[meta.Name] = value
				}
				for name, value := range relation.Metas {
					metaValue := nameOrigin[name]
					if value == nil {
						if metaValue == nil || metaValue.DeleteTime != nil {
							continue
						}
						e.Exec("update object_relation_meta_value set delete_time = now() where meta_id = ? and relation_type_id =? and from_object_id = ? and to_object_id = ?",
							metaValue.MetaID, metaValue.RelationTypeID, metaValue.FromObjectID, metaValue.ToObjectID,
						)
						continue
					}
					if metaValue == nil {
						m := metas.byName[name]
						if m == nil {
							continue
						}
						e.Exec("insert into object_relation_meta_value (from_object_id, relation_type_id, to_object_id, meta_id, value, create_time) VALUES (?,?,?,?,?,?)",
							fromObject.ID, relationTypeID, toObject.ID, m.ID, value.Value, time.Now(),
						)
						continue
					}
					if value.Value == metaValue.Value {
						continue
					}
					e.Exec("update object_relation_meta_value set delete_time = null , value = ? where meta_id = ? and relation_type_id =? and from_object_id = ? and to_object_id = ?",
						value.Value, metaValue.MetaID, metaValue.RelationTypeID, metaValue.FromObjectID, metaValue.ToObjectID,
					)
				}
				for name, value := range nameOrigin {
					if value.DeleteTime != nil {
						continue
					}
					_, ok := relation.Metas[name]
					if !ok {
						e.Exec("update object_relation_meta_value set delete_time = now() where meta_id = ? and relation_type_id =? and from_object_id = ? and to_object_id = ?",
							value.MetaID, value.RelationTypeID, value.FromObjectID, value.ToObjectID,
						)
					}
				}
			case 2:
				// update meta value
				meta, ok := metas.byName[fields[1]]
				if !ok || meta == nil {
					return nil, invalidArguments("no such meta: %s", fields[1])
				}
				metaValue, ok := relation.Metas[fields[1]]
				if !ok || metaValue == nil {
					e.Exec("update object_relation_meta_value set delete_time = now() where meta_id = ? and relation_type_id =? and from_object_id = ? and to_object_id = ?",
						meta.ID, relationTypeID, fromObject.ID, toObject.ID,
					)
					continue
				}
				e.Exec("update object_relation_meta_value set delete_time = null , value = ? where meta_id = ? and relation_type_id =? and from_object_id = ? and to_object_id = ?",
					metaValue.Value, meta.ID, relationTypeID, fromObject.ID, toObject.ID,
				)
			default:
				return nil, invalidArguments("path not found: %s", path)
			}
		default:
			return nil, invalidArguments("path not found: %s", path)
		}
	}
	e.Exec("update object set relation_version = ? where id in (?,?)", ts, fromObject.ID, toObject.ID)
	if e.Err != nil {
		return nil, internalError(err)
	}
	rel, err := s.getRelation(ctx, tx, relation.Relation, relation.From, relation.To, relationTypeID, fromTypeID, toTypeID, metas)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, internalError(err)
	}
	return rel, nil
}

func (s *Storage) getRelation(ctx context.Context, tx *sqlx.Tx, relationType string, from *v1.ObjectReference, to *v1.ObjectReference, relationTypeID, fromTypeID, toTypeID int, metas relationTypeMetas) (rel *v1.Relation, err error) {
	fromObject, toObject, err := s.getRelationObjects(ctx, tx, 0, fromTypeID, toTypeID, from.Name, to.Name)
	if err != nil {
		return nil, err
	}
	relation, err := s.getRelationByID(ctx, tx, relationTypeID, fromObject.ID, toObject.ID)
	if err != nil {
		return nil, err
	}
	relMetas, err := s.getRelationMetasByID(ctx, tx, relationTypeID, fromObject.ID, toObject.ID)
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

func (s *Storage) DeleteRelation(ctx context.Context, relation *v1.Relation) (updated *v1.Relation, err error) {
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
	rel, err := s.getRelation(ctx, tx, relation.Relation, relation.From, relation.To, relationTypeID, fromTypeID, toTypeID, metas)
	if err != nil {
		return nil, err
	}

	e := &sqly.Execer{
		Ctx: ctx,
		Tx:  tx,
	}

	e.Exec("insert into deleted_object_relation select * from object_relation where relation_type_id = ? and from_object_id = ? and to_object_id = ?",
		relationTypeID, fromTypeID, toTypeID)
	e.Exec("update deleted_object_relation set delete_time = now() where relation_type_id = ? and from_object_id = ? and to_object_id = ?", relationTypeID, fromTypeID, toTypeID)
	e.Exec("delete from object_relation where relation_type_id = ? and from_object_id = ? and to_object_id = ?",
		relationTypeID, fromTypeID, toTypeID)

	e.Exec("insert into deleted_object_relation_meta_value select * from object_relation_meta_value where relation_type_id = ? and from_object_id = ? and to_object_id = ?",
		relationTypeID, fromTypeID, toTypeID)
	e.Exec("update deleted_object_relation_meta_value set delete_time = now() where relation_type_id = ? and from_object_id = ? and to_object_id = ?", relationTypeID, fromTypeID, toTypeID)
	e.Exec("delete from object_relation_meta_value where relation_type_id = ? and from_object_id = ? and to_object_id = ?",
		relationTypeID, fromTypeID, toTypeID)

	e.Exec("update object set relation_version = ? where id in (?,?)", ts, fromObject.ID, toObject.ID)

	if e.Err != nil {
		return nil, internalError(err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, internalError(err)
	}
	return rel, nil
}

func (s *Storage) WatchRelation(ctx context.Context, from, to, relation string, f storage.RelationFilterWatcher) error {
	rel, err := s.cache.GetRelations(ctx, from, to, relation)
	if err != nil {
		return internalError(err)
	}
	err = rel.AddFilterWatcher(f)
	if err != nil {
		return internalError(err)
	}
	<-ctx.Done()
	rel.RemoveFilterWatcher(f)
	return nil
}
