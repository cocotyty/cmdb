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
	"github.com/zhihu/cmdb/pkg/tools/sqly"
	"google.golang.org/grpc/codes"
	errors "google.golang.org/grpc/status"
)

// GetObjectType get objectType by name.
//
func (s *Storage) GetObjectType(ctx context.Context, name string, consistent bool) (n *v1.ObjectType, err error) {
	if !consistent {
		s.cache.TypeCache(func(d *typetables.Database) {
			row, ok := d.ObjectTypeTable.GetByName(name)
			if !ok {
				err = errors.Newf(codes.NotFound, "type:%s not found", name).Err()
				return
			}
			n = convertFromCache(row)
		})
		return
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	n, _, _, _, err = s.loadTypeFromDatabase(ctx, tx, name)
	return
}

func (s *Storage) ListObjectTypes(ctx context.Context, consistent bool, showDeleted bool) (list []*v1.ObjectType, err error) {
	if !showDeleted && !consistent {
		s.cache.TypeCache(func(d *typetables.Database) {
			for _, objectType := range d.ObjectTypeTable.ID {
				list = append(list, convertFromCache(objectType))
			}
		})
		return list, nil
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var types []*model.ObjectType
	if showDeleted {
		err = tx.SelectContext(ctx, &types, "select * from object_type;")
	} else {
		err = tx.SelectContext(ctx, &types, "select * from object_type where delete_time is null;")
	}
	if err != nil {
		return nil, err
	}
	list, err = s.loadTypesFromDatabase(ctx, tx, types)
	return list, err
}

func (s *Storage) DeleteObjectType(ctx context.Context, name string) (n *v1.ObjectType, err error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	n, typ, _, _, err := s.loadTypeFromDatabase(ctx, tx, name)
	if err != nil {
		return nil, err
	}
	recordDB := &EventRecordDB{
		ctx: ctx,
		tx:  tx,
	}
	now := time.Now()
	typ.DeleteTime = &now
	err = recordDB.UpdateObjectType(typ)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	s.cache.WriteCache(func(d *typetables.Database) {
		d.OnEvents(recordDB.changes)
	})
	return n, nil
}

func (s *Storage) ForceDeleteObjectType(ctx context.Context, name string) (n *v1.ObjectType, err error) {
	tx, err := s.db.Beginx()
	if err != nil {
		return
	}
	defer tx.Rollback()
	n, objectType, _, statuses, err := s.loadTypeFromDatabase(ctx, tx, name)
	if err != nil {
		return nil, err
	}
	e := &sqly.Execer{Ctx: ctx, Tx: tx}

	for _, status := range statuses {
		e.Exec("delete from object_state where status_id = ?", status.ID)
	}
	e.Exec("delete from object_status where type_id = ?", objectType.ID)
	e.Exec("delete from object_meta where type_id = ?", objectType.ID)
	e.Exec("delete from object_log where object_id in (select id from object where type_id = ?)", objectType.ID)
	e.Exec("delete from object_meta_value where object_id in (select id from object where type_id = ?)", objectType.ID)
	e.Exec("delete from object_relation where relation_type_id in (select id from object_relation_type where object_relation_type.from_type_id = ? or to_type_id = ?)", objectType.ID, objectType.ID)
	e.Exec("delete from object_relation_meta_value where relation_type_id  in (select id from object_relation_type where object_relation_type.from_type_id = ? or to_type_id = ?)", objectType.ID, objectType.ID)
	e.Exec("delete from object_relation_meta where relation_type_id  in (select id from object_relation_type where from_type_id = ? or to_type_id = ?)", objectType.ID, objectType.ID)
	e.Exec("delete from object_relation_type where from_type_id = ? or to_type_id = ?", objectType.ID, objectType.ID)
	e.Exec("delete from object_type where id = ?", objectType.ID)
	err = e.Err
	if err != nil {
		return nil, err
	}
	tx.Commit()
	return n, nil
}

func (s *Storage) CreateObjectType(ctx context.Context, typ *v1.ObjectType) (n *v1.ObjectType, err error) {
	now := time.Now()
	tx, err := s.db.Beginx()
	if err != nil {
		return
	}
	defer tx.Rollback()

	recorder := eventRecord(ctx, tx)

	var typRow = &model.ObjectType{
		Name:        typ.Name,
		Description: typ.Description,
		CreateTime:  now,
	}
	err = recorder.InsertObjectType(typRow)
	if err != nil {
		return nil, err
	}
	for _, meta := range typ.Metas {
		var metaRow = &model.ObjectMeta{
			TypeID:      typRow.ID,
			Name:        meta.Name,
			ValueType:   int(meta.ValueType),
			Description: meta.Description,
			CreateTime:  now,
		}
		err = recorder.InsertObjectMeta(metaRow)
		if err != nil {
			return nil, err
		}
	}
	for _, status := range typ.Statuses {
		err = s.InsertAllObjectTypeStatus(recorder, status, typRow.ID, now)
		if err != nil {
			return nil, err
		}
	}
	typ.CreateTime, _ = ptypes.TimestampProto(now)
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	s.cache.WriteCache(func(m *typetables.Database) {
		m.OnEvents(recorder.changes)
	})
	return typ, nil
}

func (s *Storage) InsertAllObjectTypeStatus(recorder *EventRecordDB, status *v1.ObjectStatus, typeID int, now time.Time) (err error) {
	statusRow := &model.ObjectStatus{
		TypeID:      typeID,
		Name:        status.Name,
		Description: status.Description,
		CreateTime:  now,
	}
	err = recorder.InsertObjectStatus(statusRow)
	if err != nil {
		return err
	}
	for _, state := range status.States {
		stateRow := &model.ObjectState{
			StatusID:    statusRow.ID,
			Name:        state.Name,
			Description: state.Description,
			CreateTime:  now,
		}
		err := recorder.InsertObjectState(stateRow)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) loadTypesFromDatabase(ctx context.Context, tx *sqlx.Tx, types []*model.ObjectType) (list []*v1.ObjectType, err error) {
	var typIDs = make([]int, 0, len(types))
	idTypes := make(map[int]*v1.ObjectType, len(types))
	list = make([]*v1.ObjectType, 0, len(types))
	for _, objectType := range types {
		typ := convertObjectType(objectType)
		idTypes[objectType.ID] = typ
		list = append(list, typ)
		typIDs = append(typIDs, objectType.ID)

	}
	query, args, _ := sqlx.In("select * from object_meta where type_id in(?) and delete_time is null", typIDs)
	var metas []*model.ObjectMeta
	err = tx.SelectContext(ctx, &metas, query, args...)
	if err != nil {
		return nil, err
	}
	for _, meta := range metas {
		typ, ok := idTypes[meta.TypeID]
		if !ok {
			continue
		}
		if typ.Metas == nil {
			typ.Metas = make(map[string]*v1.ObjectMeta)
		}
		typ.Metas[meta.Name] = convertMeta(meta)
	}
	query, args, _ = sqlx.In("select * from object_status where type_id in (?) and delete_time is null", typIDs)
	var statuses []*model.ObjectStatus
	err = tx.SelectContext(ctx, &statuses, query, args...)
	if err != nil {
		return nil, err
	}
	var states []*model.ObjectState
	query, args, _ = sqlx.In("select object_state.* from object_state inner join object_status os on object_state.status_id = os.id where os.type_id in (?) and object_state.delete_time is null", typIDs)
	err = tx.SelectContext(ctx, &states, query, args...)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		for _, status := range statuses {
			if status.States == nil {
				status.States = map[string]*model.ObjectState{}
			}
			status.States[state.Name] = state
		}
	}
	for _, status := range statuses {
		typ, ok := idTypes[status.TypeID]
		if !ok {
			continue
		}
		if typ.Statuses == nil {
			typ.Statuses = make(map[string]*v1.ObjectStatus)
		}
		typ.Statuses[status.Name] = convertStatus(status)
	}
	return
}

func (s *Storage) loadTypeFromDatabase(ctx context.Context, tx *sqlx.Tx, name string) (*v1.ObjectType, *model.ObjectType, []*model.ObjectMeta, map[string]*model.ObjectStatus, error) {
	var typ = &model.ObjectType{}
	err := tx.GetContext(ctx, typ, "select * from object_type where name = ?", name)
	if err != nil {
		if err == sql.ErrNoRows {
			err = errors.Newf(codes.NotFound, "no such object_type: %s", name).Err()
		}
		return nil, nil, nil, nil, err
	}
	var metas []*model.ObjectMeta
	err = tx.SelectContext(ctx, &metas, "select * from object_meta where type_id = ? and delete_time is null", typ.ID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	statuses, err := s.loadStatuses(ctx, tx, typ.ID)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	var objectType = convertObjectType(typ)
	objectType.Metas = make(map[string]*v1.ObjectMeta, len(metas))
	for _, meta := range metas {
		objectType.Metas[meta.Name] = convertMeta(meta)
	}
	objectType.Statuses = make(map[string]*v1.ObjectStatus, len(statuses))
	for _, status := range statuses {
		objectType.Statuses[status.Name] = convertStatus(status)
	}
	return objectType, typ, metas, statuses, nil
}

func (s *Storage) UpdateObjectType(ctx context.Context, paths []string, typ *v1.ObjectType) (n *v1.ObjectType, err error) {
	now := time.Now()
	tx, err := s.db.Beginx()
	if err != nil {
		return
	}
	defer tx.Rollback()
	recorder := eventRecord(ctx, tx)
	var t model.ObjectType
	err = tx.GetContext(ctx, &t, "select * from object_type where name = ? limit 1 for update", typ.Name)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.Newf(codes.NotFound, "no such type: %s", typ.Name).Err()
		}
		return nil, err
	}
	if len(paths) == 0 {
		paths = []string{
			"description", "metas", "statuses",
		}
	}
	for _, path := range paths {
		names := strings.Split(path, ".")
		switch names[0] {
		case "description":
			t.Description = typ.Description
			err = recorder.UpdateObjectType(&t)
			if err != nil {
				return nil, err
			}
		case "metas":
			if len(names) == 1 {
				err = s.updateObjectTypeMetas(ctx, tx, recorder, &t, typ)
				if err != nil {
					return nil, err
				}
				continue
			}
			if len(names) == 2 {
				err = s.updateObjectTypeMeta(ctx, tx, recorder, &t, typ.Metas[names[1]])
				if err != nil {
					return nil, err
				}
				continue
			}
			if len(names) == 3 {
				err = s.updateObjectTypeMetaField(ctx, tx, recorder, &t, typ.Metas[names[1]], names[2])
				if err != nil {
					return nil, err
				}
				continue
			}
		case "statuses":
			if len(names) == 1 {
				err = s.updateObjectTypeStatues(ctx, tx, recorder, t.ID, typ.Statuses)
				if err != nil {
					return nil, err
				}
				continue
			}
			// statuses.{{status_name}}
			if len(names) == 2 {
				var name = names[1]
				origin, err := s.getObjectTypeStatus(ctx, tx, t.ID, name, true)
				if err != nil {
					return nil, err
				}
				status := typ.Statuses[name]
				if status == nil {
					origin.DeleteTime = &now
					err = recorder.UpdateObjectStatus(origin)
					if err != nil {
						return nil, err
					}
					continue
				}
				origin.Description = status.Description
				err = s.updateObjectTypeStatus(recorder, origin, status)
				if err != nil {
					return nil, err
				}
				continue
			}
			// statuses.{{status_name}}.{{field}}
			if len(names) == 3 {
				statusName := names[1]
				status, ok := typ.Statuses[statusName]
				if !ok || status == nil {
					return nil, PathNotFoundError(path)
				}
				origin, err := s.getObjectTypeStatus(ctx, tx, t.ID, status.Name, true)
				if err != nil {
					return nil, err
				}
				switch names[2] {
				case "states":
					err := s.updateObjectTypeStatusStates(recorder, origin, status)
					if err != nil {
						return nil, err
					}
				case "description":
					err := s.updateObjectTypeStatusDescription(recorder, origin, status)
					if err != nil {
						return nil, err
					}
				default:
					return nil, PathNotFoundError(path)
				}
				continue
			}
			// statuses.{{status_name}}.states.{{state_name}}
			if len(names) == 4 {
				if names[2] != "states" {
					return nil, PathNotFoundError(path)
				}
				statusName := names[1]
				stateName := names[3]
				origin, err := s.getObjectTypeState(ctx, tx, statusName, stateName)
				if err == sql.ErrNoRows {
					err = nil
				}
				if err != nil {
					return nil, err
				}
				status, ok := typ.Statuses[statusName]
				if !ok || status == nil {
					// delete state
					if origin == nil {
						continue
					}
					err = s.deleteObjectTypeState(recorder, origin)
					if err != nil {
						return nil, err
					}
					continue
				}
				state, ok := status.States[stateName]
				if !ok || state == nil {
					if origin == nil {
						continue
					}
					err = s.deleteObjectTypeState(recorder, origin)
					if err != nil {
						return nil, err
					}
					continue
				}
				if origin == nil {
					originStatus, err := s.getObjectTypeStatus(ctx, tx, t.ID, statusName, false)
					if err == sql.ErrNoRows {
						return nil, errors.Newf(codes.NotFound, "not found such status: %s", statusName).Err()
					}
					if err != nil {
						return nil, err
					}
					err = s.insertObjectTypeState(recorder, originStatus.ID, state)
					if err != nil {
						return nil, err
					}
				}
				err = s.updateObjectTypeState(recorder, origin, state)
				if err != nil {
					return nil, err
				}
				continue
			}
			// statuses.{{status_name}}.states.{{state_name}}.{{field}}
			if len(names) == 5 {
				if names[2] != "states" {
					return nil, PathNotFoundError(path)
				}
				statusName := names[1]
				stateName := names[3]
				field := names[4]
				if field != "description" {
					return nil, PathNotFoundError(path)
				}
				status := typ.Statuses[statusName]
				if status == nil {
					return nil, PathNotFoundError(path)
				}

				state := status.States[stateName]
				if state == nil {
					return nil, PathNotFoundError(path)
				}
				origin, err := s.getObjectTypeState(ctx, tx, statusName, stateName)
				if err != nil {
					if err == sql.ErrNoRows {
						return nil, errors.Newf(codes.NotFound, "state %s not found", stateName).Err()
					}
					return nil, err
				}
				err = s.updateObjectTypeState(recorder, origin, state)
				if err != nil {
					return nil, err
				}
				continue
			}
		}
	}
	n, _, _, _, err = s.loadTypeFromDatabase(ctx, tx, typ.Name)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	s.cache.WriteCache(func(d *typetables.Database) {
		d.OnEvents(recorder.changes)
	})
	return n, nil
}

func (s *Storage) updateObjectTypeMetas(ctx context.Context, tx *sqlx.Tx, recoder *EventRecordDB, t *model.ObjectType, typ *v1.ObjectType) (err error) {
	now := time.Now()
	var metas []*model.ObjectMeta

	err = tx.SelectContext(ctx, &metas, "select * from object_meta where type_id = ?", t.ID)
	if err != nil {
		return
	}
	originMetas := make(map[string]*model.ObjectMeta, len(metas))
	for _, meta := range metas {
		m := typ.Metas[meta.Name]
		if m == nil {
			if meta.DeleteTime == nil {
				// delete meta
				meta.DeleteTime = &now
				err = recoder.UpdateObjectMeta(meta)
				if err != nil {
					return err
				}
			}
			continue
		}
		originMetas[meta.Name] = meta
		if meta.Description != m.Description || meta.ValueType != int(m.ValueType) {
			// update meta
			meta.Description = m.Description
			meta.ValueType = int(m.ValueType)
			err = recoder.UpdateObjectMeta(meta)
			if err != nil {
				return err
			}
			continue
		}
	}
	for name, meta := range typ.Metas {
		if meta == nil {
			continue
		}
		_, ok := originMetas[name]
		if !ok {
			// create meta
			m := &model.ObjectMeta{
				TypeID:      t.ID,
				Name:        meta.Name,
				ValueType:   int(meta.ValueType),
				Description: meta.Description,
				CreateTime:  now,
			}
			err := recoder.InsertObjectMeta(m)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Storage) updateObjectTypeMeta(ctx context.Context, tx *sqlx.Tx, recoder *EventRecordDB, t *model.ObjectType, meta *v1.ObjectMeta) (err error) {
	now := time.Now()
	var m = &model.ObjectMeta{}
	err = tx.GetContext(ctx, m, "select * from object_meta where type_id = ? and name = ?", t.ID, meta.Name)
	if err == sql.ErrNoRows {
		err = nil
	}
	if err != nil {
		return
	}
	if meta == nil {
		if m.ID == 0 {
			// do nothing
			return nil
		}
		m.DeleteTime = &now
		err = recoder.UpdateObjectMeta(m)
		return err
	}
	if m.ID == 0 {
		// create meta
		m.TypeID = t.ID
		m.Name = meta.Name
		m.ValueType = int(meta.ValueType)
		m.Description = meta.Description
		err = recoder.InsertObjectMeta(m)
		return err
	}
	m.ValueType = int(meta.ValueType)
	m.Description = meta.Description
	m.DeleteTime = nil
	return recoder.UpdateObjectMeta(m)
}

func (s *Storage) updateObjectTypeMetaField(ctx context.Context, tx *sqlx.Tx, recorder *EventRecordDB, t *model.ObjectType, meta *v1.ObjectMeta, field string) (err error) {
	if meta == nil {
		return
	}
	var m = &model.ObjectMeta{}
	err = tx.GetContext(ctx, m, "select * from object_meta where type_id = ? and name = ?", t.ID, meta.Name)
	if err == sql.ErrNoRows {
		err = nil
		return
	}
	if err != nil {
		return
	}
	switch field {
	case "description":
		m.DeleteTime = nil
		m.Description = meta.Description
		return recorder.UpdateObjectMeta(m)
	case "value_type":
		m.DeleteTime = nil
		m.ValueType = int(meta.ValueType)
		return recorder.UpdateObjectMeta(m)
	}
	return nil
}

func (s *Storage) loadStatuses(ctx context.Context, tx *sqlx.Tx, typeID int) (namedStatus map[string]*model.ObjectStatus, err error) {
	var statuses []*model.ObjectStatus
	err = tx.SelectContext(ctx, &statuses, "select * from object_status where type_id = ? and delete_time is null;", typeID)
	if err != nil {
		return nil, err
	}
	var statusesIDs = make([]int, 0, len(statuses))
	for _, status := range statuses {
		statusesIDs = append(statusesIDs, status.ID)
	}
	query, args, err := sqlx.In("select * from object_state where status_id in (?) and delete_time is null", statusesIDs)
	var states []*model.ObjectState
	err = tx.SelectContext(ctx, &states, query, args...)
	if err != nil {
		return nil, err
	}
	namedStatus = make(map[string]*model.ObjectStatus, len(statuses))
	for _, status := range statuses {
		namedStatus[status.Name] = status
		status.States = map[string]*model.ObjectState{}
		for _, state := range states {
			if state.StatusID == status.ID {
				status.States[state.Name] = state
			}
		}
	}
	return
}

func (s *Storage) updateObjectTypeStatues(ctx context.Context, tx *sqlx.Tx, recorder *EventRecordDB, tID int, updateStatuses map[string]*v1.ObjectStatus) (err error) {
	now := time.Now()
	namedStatus, err := s.loadStatuses(ctx, tx, tID)
	for _, status := range updateStatuses {
		origin := namedStatus[status.Name]
		if origin == nil {
			if status != nil {
				// insert status all
				err = s.InsertAllObjectTypeStatus(recorder, status, tID, now)
				if err != nil {
					return
				}
			}
			continue
		}

		if status == nil {
			if origin.DeleteTime == nil {
				// delete status
				origin.DeleteTime = &now
				err = recorder.UpdateObjectStatus(origin)
				if err != nil {
					return err
				}
			}
			continue
		}
		err = s.updateObjectTypeStatus(recorder, origin, status)
		if err != nil {
			return err
		}
	}
	for _, status := range namedStatus {
		_, ok := updateStatuses[status.Name]
		if !ok {
			// delete status
			err = s.deleteObjectTypeStatus(recorder, status)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Storage) getObjectTypeStatus(ctx context.Context, tx *sqlx.Tx, typID int, name string, loadStates bool) (status *model.ObjectStatus, err error) {
	status = &model.ObjectStatus{}
	err = tx.GetContext(ctx, status, "select * from object_status where type_id = ? and name = ? limit 1", typID, name)
	if err != nil {
		return nil, err
	}
	if !loadStates {
		return status, nil
	}
	var states []*model.ObjectState
	err = tx.SelectContext(ctx, &states, "select * from object_state where status_id = ? and delete_time is null", status.ID)
	if err != nil {
		return nil, err
	}
	status.States = make(map[string]*model.ObjectState, len(states))
	for _, state := range states {
		status.States[state.Name] = state
	}
	return status, nil
}

func (s *Storage) deleteObjectTypeStatus(recorder *EventRecordDB, status *model.ObjectStatus) (err error) {
	now := time.Now()
	status.DeleteTime = &now
	return recorder.UpdateObjectStatus(status)
}

func (s *Storage) updateObjectTypeStatus(recorder *EventRecordDB, origin *model.ObjectStatus, status *v1.ObjectStatus) (err error) {

	err = s.updateObjectTypeStatusDescription(recorder, origin, status)
	if err != nil {
		return err
	}
	return s.updateObjectTypeStatusStates(recorder, origin, status)
}

func (s *Storage) updateObjectTypeStatusDescription(recorder *EventRecordDB, origin *model.ObjectStatus, status *v1.ObjectStatus) (err error) {
	if origin.Description != status.Description {
		origin.Description = status.Description
		return recorder.UpdateObjectStatus(origin)
	}
	return
}

func (s *Storage) updateObjectTypeStatusStates(recorder *EventRecordDB, origin *model.ObjectStatus, status *v1.ObjectStatus) (err error) {
	for _, origin := range origin.States {
		state, ok := status.States[origin.Name]
		if !ok || state == nil {
			// delete state
			err = s.deleteObjectTypeState(recorder, origin)
			if err != nil {
				return
			}
			continue
		}
		err = s.updateObjectTypeState(recorder, origin, state)
		if err != nil {
			return
		}
	}
	for _, state := range status.States {
		_, ok := origin.States[state.Name]
		if !ok {
			err = s.insertObjectTypeState(recorder, origin.ID, state)
			if err != nil {
				return
			}
		}
	}
	return nil
}

func (s *Storage) deleteObjectTypeStateByName(recorder *EventRecordDB, statusName string, stateName string) (err error) {
	state, err := s.getObjectTypeState(recorder.ctx, recorder.tx, statusName, stateName)
	if err == sql.ErrNoRows {
		return nil
	}
	return s.deleteObjectTypeState(recorder, state)
}

func (s *Storage) deleteObjectTypeState(recorder *EventRecordDB, state *model.ObjectState) (err error) {
	if state.DeleteTime != nil {
		return nil
	}
	now := time.Now()
	state.DeleteTime = &now
	return recorder.UpdateObjectState(state)
}

func (s *Storage) getObjectTypeState(ctx context.Context, tx *sqlx.Tx, statusName string, stateName string) (_ *model.ObjectState, err error) {
	var state model.ObjectState
	err = tx.GetContext(ctx, &state, "select object_state.* from object_state left join object_status os on object_state.status_id = os.id where os.name = ? and object_state.name = ? limit 1", statusName, stateName)
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (s *Storage) updateObjectTypeState(recorder *EventRecordDB, origin *model.ObjectState, state *v1.ObjectState) (err error) {
	if origin.Description != state.Description {
		origin.Description = state.Description
		return recorder.UpdateObjectState(origin)
	}
	return
}

func (s *Storage) insertObjectTypeState(recoder *EventRecordDB, statusID int, state *v1.ObjectState) (err error) {
	return recoder.InsertObjectState(&model.ObjectState{
		StatusID:    statusID,
		Name:        state.Name,
		Description: state.Description,
		CreateTime:  time.Now(),
	})
}
