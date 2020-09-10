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
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"github.com/zhihu/cmdb/pkg/model"
	"github.com/zhihu/cmdb/pkg/model/typetables"
	"google.golang.org/grpc/codes"
	errors "google.golang.org/grpc/status"
)

func (s *Storage) CreateRelationType(ctx context.Context, typ *v1.RelationType) (*v1.RelationType, error) {
	if typ.Name == "" {
		return nil, errors.Newf(codes.InvalidArgument, "type name must not be empty").Err()
	}
	now := time.Now()
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()
	var fromTypeID, toTypeID int

	s.cache.TypeCache(func(d *typetables.Database) {
		fromType, ok := d.ObjectTypeTable.GetByName(typ.From)
		if !ok {
			return
		}
		toType, ok := d.ObjectTypeTable.GetByName(typ.To)
		if !ok {
			return
		}
		fromTypeID = fromType.ID
		toTypeID = toType.ID
	})
	relationType, err := s.insertRelationType(ctx, tx, fromTypeID, toTypeID, typ.Name, typ.Description, now)
	if err != nil {
		return nil, err
	}
	for _, meta := range typ.Metas {
		err = s.insertRelationMeta(ctx, tx, relationType.ID, meta, now)
		if err != nil {
			return nil, err
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, internalError(err)
	}
	typ.CreateTime, _ = ptypes.TimestampProto(now)
	typ.DeleteTime = nil
	typ.UpdateTime = nil
	return typ, nil
}

func (s *Storage) insertRelationType(ctx context.Context, tx *sqlx.Tx, fromTypeID int, toTypeID int, name string, description string, now time.Time) (typ *model.ObjectRelationType, err error) {
	result, err := tx.ExecContext(ctx,
		"insert into object_relation_type (from_type_id, to_type_id, name, description, create_time) VALUES (?,?,?,?,?)",
		fromTypeID, toTypeID, name, description, now,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "duplicate") {
			return nil, errors.Newf(codes.AlreadyExists, "relation %s already exist", name).Err()
		}
		return nil, internalError(err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, internalError(err)
	}
	typ = &model.ObjectRelationType{
		ID:          int(id),
		FromTypeID:  fromTypeID,
		ToTypeID:    toTypeID,
		Name:        name,
		Description: description,
		CreateTime:  now,
		UpdateTime:  nil,
		DeleteTime:  nil,
	}
	s.cache.WriteCache(func(d *typetables.Database) {
		d.InsertObjectRelationType(typ)
	})
	return typ, nil
}

func (s *Storage) insertRelationMeta(ctx context.Context, tx *sqlx.Tx, typID int, meta *v1.ObjectMeta, now time.Time) error {
	res, err := tx.ExecContext(ctx,
		"insert into object_relation_meta (relation_type_id, name, value_type, description, create_time) VALUES (?,?,?,?,?)",
		typID, meta.Name, meta.ValueType, meta.Description, now,
	)

	if err != nil {
		return internalError(err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return err
	}
	s.cache.WriteCache(func(d *typetables.Database) {
		d.InsertObjectRelationMeta(&model.ObjectRelationMeta{
			ID:             int(id),
			RelationTypeID: typID,
			Name:           meta.Name,
			ValueType:      int(meta.ValueType),
			Description:    meta.Description,
			CreateTime:     now,
			DeleteTime:     nil,
		})
	})
	return nil
}

func (s *Storage) getRelationTypeFromToTypeID(from string, to string) (fromTypeID, toTypeID int) {
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
	})
	return
}

func (s *Storage) UpdateRelationType(ctx context.Context, typ *v1.RelationType, paths []string) (*v1.RelationType, error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()
	rt, err := s.getDatabaseRelationType(ctx, tx, typ.Name, typ.From, typ.To)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		paths = []string{
			"description", "metas",
		}
	}
	for _, path := range paths {
		fields := strings.Split(path, ".")
		switch fields[0] {
		case "description":
			err = s.updateRelationTypeDesc(ctx, tx, rt.ID, fields[0])
			if err != nil {
				return nil, err
			}
		case "metas":
			switch len(fields) {
			case 1:
				err = s.updateRelationTypeMetas(ctx, tx, rt.ID, typ.Metas)
			case 2:
				// metas.*
				err = s.updateRelationTypeMeta(ctx, tx, rt.ID, typ.Metas[fields[1]])
			case 3:
				// metas.*.*
				err = s.updateRelationTypeMetaField(ctx, tx, rt.ID, typ.Metas[fields[1]], fields[2])
			}
			if err != nil {
				return nil, err
			}
		}
	}
	relationType, err := s.getRelationType(ctx, tx, typ.Name, typ.From, typ.To)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return relationType, nil
}

func (s *Storage) updateRelationTypeDesc(ctx context.Context, tx *sqlx.Tx, typeID int, description string) error {
	_, err := tx.ExecContext(ctx, "update object_relation_type set description = ?, update_time=now() where id = ?", description, typeID)
	if err != nil {
		return internalError(err)
	}
	return nil
}

func (s *Storage) updateRelationTypeMetas(ctx context.Context, tx *sqlx.Tx, typeID int, metas map[string]*v1.ObjectMeta) error {
	var relationMetas []*model.ObjectRelationMeta
	err := tx.SelectContext(ctx, &relationMetas, "select * from object_relation_meta where relation_type_id = ?", typeID)
	if err != nil {
		return internalError(err)
	}
	var originMetas = make(map[string]*model.ObjectRelationMeta, len(relationMetas))
	for _, meta := range relationMetas {
		originMetas[meta.Name] = meta
	}
	for _, meta := range metas {
		exist, ok := originMetas[meta.Name]
		if !ok {
			err = s.insertRelationMeta(ctx, tx, typeID, meta, time.Now())
			if err != nil {
				return err
			}
			continue
		}
		if meta == nil {
			if exist.DeleteTime == nil {
				err = s.deleteRelationTypeMeta(ctx, tx, exist)
				if err != nil {
					return err
				}
			}
			continue
		}
		if v1.ValueType(exist.ValueType) != meta.ValueType || exist.Description != meta.Description {
			err = s.updateRelationTypeMeta(ctx, tx, typeID, meta)
			if err != nil {
				return err
			}
			continue
		}
	}

	for _, meta := range originMetas {
		if metas[meta.Name] == nil && meta.DeleteTime == nil {
			err = s.deleteRelationTypeMeta(ctx, tx, meta)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Storage) deleteRelationTypeMeta(ctx context.Context, tx *sqlx.Tx, meta *model.ObjectRelationMeta) error {
	_, err := tx.ExecContext(ctx, "update object_relation_meta set delete_time = now() where id = ?", meta.ID)
	if err != nil {
		return internalError(err)
	}
	s.cache.WriteCache(func(d *typetables.Database) {
		d.DeleteObjectRelationMeta(meta)
	})
	return internalError(err)
}

func (s *Storage) updateRelationTypeMeta(ctx context.Context, tx *sqlx.Tx, typeID int, meta *v1.ObjectMeta) error {
	_, err := tx.ExecContext(ctx, "update object_relation_meta set description = ? , value_type = ?, delete_time = null where relation_type_id = ? and name =? ", meta.Description, meta.ValueType, typeID, meta.Name)
	if err != nil {
		return internalError(err)
	}
	err = s.reloadRelationMeta(ctx, tx, typeID, meta)
	return internalError(err)
}

func (s *Storage) updateRelationTypeMetaField(ctx context.Context, tx *sqlx.Tx, typeID int, meta *v1.ObjectMeta, field string) error {
	var err error
	switch field {
	case "description":
		_, err = tx.ExecContext(ctx, "update object_relation_meta set description = ? , delete_time = null where relation_type_id = ? and name = ?", meta.Description, meta.ValueType, typeID, meta.Name)
	case "value_type":
		_, err = tx.ExecContext(ctx, "update object_relation_meta set value_type = ?, delete_time = null where relation_type_id = ? and name = ?", meta.Description, meta.ValueType, typeID, typeID, meta.Name)
	}
	if err != nil {
		return internalError(err)
	}
	err = s.reloadRelationMeta(ctx, tx, typeID, meta)
	return internalError(err)
}

func (s *Storage) reloadRelationMeta(ctx context.Context, tx *sqlx.Tx, typeID int, meta *v1.ObjectMeta) error {
	var row model.ObjectRelationMeta
	err := tx.SelectContext(ctx, &row, "select * from object_relation_meta where where relation_type_id = ? and name = ?", typeID, meta.Name)
	if err != nil {
		return internalError(err)
	}
	s.cache.WriteCache(func(d *typetables.Database) {
		d.UpdateObjectRelationMeta(&row)
	})
	return nil
}

func (s *Storage) getDatabaseRelationType(ctx context.Context, tx *sqlx.Tx, name string, from string, to string) (*model.ObjectRelationType, error) {
	var rt model.ObjectRelationType
	fromTypeID, toTypeID := s.getRelationTypeFromToTypeID(from, to)
	err := tx.GetContext(ctx, &rt, "select * from object_relation_type where name = ? and from_type_id = ? and to_type_id = ?", name, fromTypeID, toTypeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, notFound("no such relation type: %s", name)
		}
		return nil, internalError(err)
	}
	return &rt, nil
}

func (s *Storage) getRelationType(ctx context.Context, tx *sqlx.Tx, name string, from string, to string) (*v1.RelationType, error) {
	rt, err := s.getDatabaseRelationType(ctx, tx, name, from, to)
	if err != nil {
		return nil, err
	}
	typ := convertRelationType(rt, from, to)
	metas, err := s.getRelationTypeMetas(ctx, tx, rt.ID)
	if err != nil {
		return nil, err
	}
	typ.Metas = metas
	return typ, nil
}

func (s *Storage) GetRelationType(ctx context.Context, name string, from string, to string) (*v1.RelationType, error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()
	typ, err := s.getRelationType(ctx, tx, name, from, to)
	if err != nil {
		return nil, err
	}
	return typ, nil
}

func (s *Storage) getRelationTypeMetas(ctx context.Context, tx *sqlx.Tx, typeID int) (map[string]*v1.ObjectMeta, error) {
	var metas []*model.ObjectRelationMeta
	err := tx.SelectContext(ctx, &metas, "select * from object_relation_meta where relation_type_id = ? and delete_time is null", typeID)
	if err != nil {
		return nil, err
	}
	nameMetas := make(map[string]*v1.ObjectMeta, len(metas))
	for _, meta := range metas {
		nameMetas[meta.Name] = &v1.ObjectMeta{
			Name:        meta.Name,
			Description: meta.Description,
			ValueType:   v1.ValueType(meta.ValueType),
		}
	}
	return nameMetas, nil
}

func (s *Storage) listRelationTypeFromCache() (list []*v1.RelationType) {
	s.cache.TypeCache(func(d *typetables.Database) {
		list = make([]*v1.RelationType, len(d.ObjectRelationTypeTable.ID))
		for _, relationType := range d.ObjectRelationTypeTable.ID {
			fromTyp, ok := d.ObjectTypeTable.GetByID(relationType.FromTypeID)
			if !ok {
				continue
			}
			toTyp, ok := d.ObjectTypeTable.GetByID(relationType.ToTypeID)
			if !ok {
				continue
			}
			typ := convertRelationType(&relationType.ObjectRelationType, fromTyp.Name, toTyp.Name)
			typ.Metas = make(map[string]*v1.ObjectMeta, len(relationType.ObjectRelationMeta))
			for _, meta := range relationType.ObjectRelationMeta {
				typ.Metas[meta.Name] = &v1.ObjectMeta{
					Name:        meta.Name,
					Description: meta.Description,
					ValueType:   v1.ValueType(meta.ValueType),
				}
			}
			list = append(list, typ)
		}
	})
	return
}

func (s *Storage) ListRelationType(ctx context.Context, consistent bool, showDeleted bool) (list []*v1.RelationType, err error) {
	if !consistent && !showDeleted {
		return s.listRelationTypeFromCache(), nil
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	var relationTypes []*model.ObjectRelationType
	if !showDeleted {
		err = tx.SelectContext(ctx, &relationTypes, "select * from object_relation_type where delete_time is null")
	} else {
		err = tx.SelectContext(ctx, &relationTypes, "select * from object_relation_type")
	}
	if err != nil {
		return nil, internalError(err)
	}
	if len(relationTypes) == 0 {
		return
	}
	var typeIDs []int
	for _, relationType := range relationTypes {
		typeIDs = append(typeIDs, relationType.FromTypeID, relationType.ToTypeID)
	}
	typeNames, err := s.getTypeNames(ctx, tx, typeIDs)
	if err != nil {
		return nil, err
	}

	var metas []*model.ObjectRelationMeta
	err = tx.SelectContext(ctx, &metas,
		"select orm.* from object_relation_meta orm left join object_relation_type ort on ort.id = orm.relation_type_id where ort.delete_time is null and orm.delete_time is null")
	if err != nil {
		return nil, internalError(err)
	}
	nameTypes := make(map[int]*v1.RelationType, len(relationTypes))
	for _, relationType := range relationTypes {
		from, ok := typeNames[relationType.FromTypeID]
		if !ok {
			continue
		}
		to, ok := typeNames[relationType.ToTypeID]
		if !ok {
			continue
		}

		typ := convertRelationType(relationType, from, to)
		typ.Metas = map[string]*v1.ObjectMeta{}
		nameTypes[relationType.ID] = typ
	}
	for _, meta := range metas {
		typ, ok := nameTypes[meta.RelationTypeID]
		if !ok {
			continue
		}
		typ.Metas[meta.Name] = &v1.ObjectMeta{
			Name:        meta.Name,
			Description: meta.Description,
			ValueType:   v1.ValueType(meta.ValueType),
		}
	}
	list = make([]*v1.RelationType, 0, len(nameTypes))
	for _, relationType := range nameTypes {
		list = append(list, relationType)
	}
	_ = tx.Commit()
	return list, nil
}

func (s *Storage) getTypeNames(ctx context.Context, tx *sqlx.Tx, ids []int) (map[int]string, error) {
	query, args, _ := sqlx.In("select id,name from object_type where id in(?)", ids)
	var typeIDNames []*IDName
	err := tx.SelectContext(ctx, &typeIDNames, query, args...)
	if err != nil {
		return nil, internalError(err)
	}
	var idsMap = map[int]string{}
	for _, in := range typeIDNames {
		idsMap[in.ID] = in.Name
	}
	return idsMap, nil
}

type IDName struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func (s *Storage) DeleteRelationType(ctx context.Context, from string, to string, name string) (*v1.RelationType, error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, internalError(err)
	}
	defer tx.Rollback()
	objectRelationType, err := s.getDatabaseRelationType(ctx, tx, name, from, to)
	if err != nil {
		return nil, err
	}
	relationType, err := s.getRelationType(ctx, tx, name, from, to)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, "update object_relation_type set delete_time = now() where id = ? ", objectRelationType.ID)
	if err != nil {
		return nil, internalError(err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return relationType, nil
}
