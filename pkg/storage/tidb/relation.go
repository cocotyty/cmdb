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

	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"github.com/zhihu/cmdb/pkg/model"
	"github.com/zhihu/cmdb/pkg/model/typetables"
)

func (s *Storage) CreateRelation(ctx context.Context, relation *v1.Relation) (created *v1.Relation, err error) {

	var relationTypeID int
	var fromTypeID int
	var toTypeID int
	s.cache.TypeCache(func(d *typetables.Database) {
		fromType, ok := d.ObjectTypeTable.GetByName(relation.From.Type)
		if !ok {
			return
		}
		toType, ok := d.ObjectTypeTable.GetByName(relation.To.Type)
		if !ok {
			return
		}
		typ, ok := d.ObjectRelationTypeTable.GetByLogicalID(fromType.ID, toType.ID, relation.Relation)
		if !ok {
			return
		}
		fromTypeID = typ.FromTypeID
		toTypeID = typ.ToTypeID
		relationTypeID = typ.ID
	})
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
	var fromObject model.Object
	var toObject model.Object
	var objects []*model.Object

	tx.SelectContext(ctx, &objects,
		"select  * from object where ( type_id = ? and name = ? ) or ( type_id=? and name = ? ) for update",
		fromTypeID, relation.From.Name,toTypeID, relation.To.Name,
	)
	err = tx.GetContext(ctx, &fromObject, "select * from object where type_id = ? and name = ? limit 1",
		fromTypeID, relation.From.Name,
	)
	if err == sql.ErrNoRows {
		return nil, notFound("object not found: %s/%s", relation.From.Type, relation.From.Name)
	}

	err = tx.GetContext(ctx, &toObject, "select * from object where type_id = ? and name = ? limit 1",
		toTypeID, relation.To.Name,
	)
	if err == sql.ErrNoRows {
		return nil, notFound("object not found: %s/%s", relation.From.Type, relation.From.Name)
	}
	now := time.Now()
	result, err := tx.ExecContext(ctx,
		"insert into object_relation (from_object_id, relation_type_id, to_object_id, create_time) VALUES (?,?,?,now())",
		relationTypeID,
	)
	id, _ := result.LastInsertId()

	return nil, nil
}
