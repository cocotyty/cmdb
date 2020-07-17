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

	"github.com/jmoiron/sqlx"
	"github.com/zhihu/cmdb/pkg/model"
	"github.com/zhihu/cmdb/pkg/storage/cdc"
)

type EventRecordDB struct {
	ctx     context.Context
	tx      *sqlx.Tx
	changes []cdc.Event
}

func eventRecord(ctx context.Context, tx *sqlx.Tx) *EventRecordDB {
	return &EventRecordDB{
		ctx: ctx,
		tx:  tx,
	}
}

func (d *EventRecordDB) InsertObjectType(typ *model.ObjectType) error {
	result, err := d.tx.ExecContext(d.ctx,
		"insert into object_type (name, description, create_time, delete_time) VALUES (?,?,?,?)",
		typ.Name, typ.Description,
		typ.CreateTime, typ.DeleteTime,
	)
	if err != nil {
		return internalError(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return internalError(err)
	}
	typ.ID = int(id)
	row := *typ
	d.changes = append(d.changes, cdc.Event{
		Type: cdc.Create,
		Row:  &row,
	})
	return nil
}

func (d *EventRecordDB) UpdateObjectType(typ *model.ObjectType) error {
	_, err := d.tx.ExecContext(d.ctx,
		"update object_type set description = ?,  delete_time = ? where id = ?",
		typ.Name, typ.Description,
		typ.DeleteTime, typ.ID,
	)
	if err != nil {
		return internalError(err)
	}
	row := *typ
	d.changes = append(d.changes, cdc.Event{
		Type: cdc.Update,
		Row:  &row,
	})
	return nil
}

func (d *EventRecordDB) InsertObjectMeta(meta *model.ObjectMeta) error {
	result, err := d.tx.ExecContext(d.ctx, "insert into object_meta (type_id, name, value_type, description, create_time, delete_time) VALUES (?,?,?,?,?,?)",
		meta.TypeID, meta.Name, meta.ValueType, meta.Description, meta.CreateTime, meta.DeleteTime,
	)
	if err != nil {
		return internalError(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return internalError(err)
	}
	meta.ID = int(id)
	row := *meta
	d.changes = append(d.changes, cdc.Event{
		Type: cdc.Create,
		Row:  &row,
	})
	return nil
}

func (d *EventRecordDB) UpdateObjectMeta(meta *model.ObjectMeta) error {
	_, err := d.tx.ExecContext(d.ctx,
		"update object_meta set description = ?,  delete_time = ?,value_type = ? where id = ?",
		meta.Name, meta.Description,
		meta.DeleteTime, meta.ID,
	)
	if err != nil {
		return internalError(err)
	}
	row := *meta
	d.changes = append(d.changes, cdc.Event{
		Type: cdc.Update,
		Row:  &row,
	})
	return nil
}

func (d *EventRecordDB) InsertObjectStatus(status *model.ObjectStatus) error {
	result, err := d.tx.ExecContext(d.ctx, "insert into object_status (type_id, name, description, create_time) VALUES (?,?,?,?)",
		status.TypeID, status.Name, status.Description, status.CreateTime,
	)
	if err != nil {
		return internalError(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return internalError(err)
	}
	status.ID = int(id)
	row := *status
	d.changes = append(d.changes, cdc.Event{
		Type: cdc.Create,
		Row:  &row,
	})
	return nil
}

func (d *EventRecordDB) UpdateObjectStatus(status *model.ObjectStatus) error {
	_, err := d.tx.ExecContext(d.ctx, "update object_status set description = ? where id = ?",
		status.Description, status.ID,
	)
	if err != nil {
		return internalError(err)
	}

	row := *status
	d.changes = append(d.changes, cdc.Event{
		Type: cdc.Update,
		Row:  &row,
	})
	return nil
}

func (d *EventRecordDB) InsertObjectState(state *model.ObjectState) error {
	result, err := d.tx.ExecContext(d.ctx, "insert into object_state (status_id, name, description, create_time) VALUES (?,?,?,?)",
		state.StatusID, state.Name, state.Description, state.CreateTime,
	)
	if err != nil {
		return internalError(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return internalError(err)
	}
	state.ID = int(id)
	row := *state
	d.changes = append(d.changes, cdc.Event{
		Type: cdc.Create,
		Row:  &row,
	})
	return nil
}

func (d *EventRecordDB) UpdateObjectState(state *model.ObjectState) error {
	_, err := d.tx.ExecContext(d.ctx, "update object_state set description = ? where id = ?",
		state.Description, state.ID,
	)
	if err != nil {
		return internalError(err)
	}

	row := *state
	d.changes = append(d.changes, cdc.Event{
		Type: cdc.Update,
		Row:  &row,
	})
	return nil
}

func (d *EventRecordDB) Changes() []cdc.Event {
	return d.changes
}
