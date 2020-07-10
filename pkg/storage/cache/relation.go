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

package cache

import (
	"context"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"github.com/zhihu/cmdb/pkg/model"
	"github.com/zhihu/cmdb/pkg/model/relations"
	"github.com/zhihu/cmdb/pkg/model/typetables"
	"github.com/zhihu/cmdb/pkg/storage"
	"github.com/zhihu/cmdb/pkg/storage/cdc"
)

type relationID struct {
	from int
	to   int
}

type Relations struct {
	from            *Objects
	to              *Objects
	relation        string
	relationTypeID  int
	memory          relations.Database
	types           *Types
	loaded          bool
	handlers        []storage.RelationFilterWatcher
	relations       map[relationID]*v1.RichRelation
	mutex           sync.RWMutex
	bufferedEvents  [][]cdc.Event
	releaseCallback func()
	ref             *ReferenceManager
}

func NewRelations(from *Objects, to *Objects, relation string, relationTypeID int, types *Types, releaseCallback func()) *Relations {
	return &Relations{from: from, to: to, relation: relation, relationTypeID: relationTypeID, types: types, releaseCallback: releaseCallback}
}

func (t *Relations) Size() int {
	t.mutex.RLock()
	size := len(t.relations)
	t.mutex.RUnlock()
	return size
}

func (t *Relations) StartGC() {
	t.ref = NewReferenceManager(t.releaseCallback, time.Second*5, time.Second*10)
}

func (t *Relations) ResetBuffer() {
	t.bufferedEvents = nil
}

func (t *Relations) Ref() bool {
	return t.ref.Ref()
}

func (t *Relations) UnRef() {
	t.ref.UnRef()
}

func (t *Relations) LoadData(ctx context.Context, db *sqlx.DB) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var list []*model.ObjectRelation
	err = tx.SelectContext(ctx, &list, `select * from object_relation where relation_type_id = ? and delete_time is null`, t.relationTypeID)
	if err != nil {
		return err
	}
	var metas []*model.ObjectRelationMetaValue
	err = tx.SelectContext(ctx, &metas, `select * from object_relation_meta_value where relation_type_id = ?`, t.relationTypeID)
	if err != nil {
		return err
	}
	_ = tx.Commit()
	t.mutex.Lock()
	t.memory.Init()
	for _, relation := range list {
		t.memory.InsertObjectRelation(relation)
	}
	for _, meta := range metas {
		t.memory.InsertObjectRelationMetaValue(meta)
	}
	t.loaded = true
	t.relations = map[relationID]*v1.RichRelation{}
	for id, relation := range t.memory.ObjectRelationTable.ID {
		fromObject := t.from.GetByID(relation.FromObjectID)
		toObject := t.to.GetByID(relation.ToObjectID)
		rid := relationID{
			from: id.FromObjectID,
			to:   id.ToObjectID,
		}
		var rel = &v1.RichRelation{
			Relation: t.relation,
			From: &v1.Object{
				Type: t.from.name,
				Name: fromObject.Name,
			},
			To: &v1.Object{
				Type: t.to.name,
				Name: toObject.Name,
			},
			Metas: map[string]*v1.ObjectMetaValue{},
		}
		rel.CreateTime, _ = ptypes.TimestampProto(relation.CreateTime)
		if relation.UpdateTime != nil {
			rel.UpdateTime, _ = ptypes.TimestampProto(*relation.UpdateTime)
		}
		t.relations[rid] = rel
	}
	var metaTypes = map[int]*model.ObjectRelationMeta{}
	t.types.Read(func(d *typetables.Database) {
		relationMetas := d.ObjectRelationMetaTable.FilterByRelationTypeID(t.relationTypeID)
		for _, meta := range relationMetas {
			var m = meta.ObjectRelationMeta
			metaTypes[meta.ID] = &m
		}
	})

	for _, meta := range metas {
		rel, ok := t.relations[relationID{
			from: meta.FromObjectID,
			to:   meta.ToObjectID,
		}]
		if !ok {
			continue
		}
		metaType, ok := metaTypes[meta.MetaID]
		if !ok {
			continue
		}
		rel.Metas[metaType.Name] = &v1.ObjectMetaValue{
			ValueType: v1.ValueType(metaType.ValueType),
			Value:     meta.Value,
		}
	}
	t.mutex.Unlock()

	return nil
}

func (t *Relations) RemoveFilterWatcher(f storage.RelationFilterWatcher) {
	t.mutex.Lock()
	var nHandlers = make([]storage.RelationFilterWatcher, 0, len(t.handlers)-1)
	for _, handler := range t.handlers {
		if handler != f {
			nHandlers = append(nHandlers, handler)
		}
	}
	t.handlers = nHandlers
	t.ref.UnRef()
	t.mutex.Unlock()
}

func (t *Relations) AddFilterWatcher(f storage.RelationFilterWatcher) error {
	t.mutex.Lock()
	if !t.ref.Ref() {
		return ErrReleased
	}
	t.handlers = append(t.handlers, f)
	t.mutex.Unlock()
	f.OnInit(t.Filter(f))
	return nil
}

func (t *Relations) richRelation(relation *v1.RichRelation) *v1.RichRelation {
	from := t.from.GetByName(relation.From.Name)
	to := t.to.GetByName(relation.To.Name)
	var cp = &v1.RichRelation{
		Relation:   relation.Relation,
		From:       from,
		To:         to,
		Metas:      map[string]*v1.ObjectMetaValue{},
		CreateTime: relation.CreateTime,
		UpdateTime: relation.UpdateTime,
		DeleteTime: relation.DeleteTime,
	}
	for name, value := range relation.Metas {
		cp.Metas[name] = &v1.ObjectMetaValue{
			ValueType: value.ValueType,
			Value:     value.Value,
		}
	}
	return cp
}

func (t *Relations) Filter(f storage.RelationFilterWatcher) (list []*v1.RichRelation) {
	t.mutex.RLock()
	for _, relation := range t.relations {
		if f.Filter(relation) {
			from := t.from.GetByName(relation.From.Name)
			to := t.to.GetByName(relation.To.Name)
			var cp = &v1.RichRelation{
				Relation:   relation.Relation,
				From:       from,
				To:         to,
				Metas:      map[string]*v1.ObjectMetaValue{},
				CreateTime: relation.CreateTime,
				UpdateTime: relation.UpdateTime,
				DeleteTime: relation.DeleteTime,
			}
			for name, value := range relation.Metas {
				cp.Metas[name] = &v1.ObjectMetaValue{
					ValueType: value.ValueType,
					Value:     value.Value,
				}
			}
			list = append(list, cp)
		}
	}
	t.mutex.RUnlock()
	return list
}

func (t *Relations) convert(relation *model.ObjectRelation, metas map[int]*model.ObjectRelationMeta) *v1.RichRelation {
	fromObject := t.from.GetByID(relation.FromObjectID)
	toObject := t.to.GetByID(relation.ToObjectID)

	var rel = &v1.RichRelation{
		Relation: t.relation,
		From: &v1.Object{
			Type: t.from.name,
			Name: fromObject.Name,
		},
		To: &v1.Object{
			Type: t.to.name,
			Name: toObject.Name,
		},
		Metas: map[string]*v1.ObjectMetaValue{},
	}
	rel.CreateTime, _ = ptypes.TimestampProto(relation.CreateTime)
	if relation.UpdateTime != nil {
		rel.UpdateTime, _ = ptypes.TimestampProto(*relation.UpdateTime)
	}
	if metas != nil {
		rows := t.memory.ObjectRelationMetaValueTable.FilterByRelationID(relation.FromObjectID, t.relationTypeID, relation.ToObjectID)
		for _, row := range rows {
			m, ok := metas[row.MetaID]
			if !ok {
				continue
			}
			rel.Metas[m.Name] = &v1.ObjectMetaValue{
				ValueType: v1.ValueType(m.ValueType),
				Value:     row.Value,
			}
		}
	}
	return rel
}

func (t *Relations) OnEvents(events []cdc.Event) {
	var add []relationID
	var del []relationID
	var update []relationID
	var need []cdc.Event
	for _, event := range events {
		switch row := event.Row.(type) {
		case *model.ObjectRelation:
			if row.RelationTypeID != t.relationTypeID {
				continue
			}
			need = append(need, event)
			rid := relationID{
				from: row.FromObjectID,
				to:   row.ToObjectID,
			}
			switch event.Type {
			case cdc.Create:
				add = append(add, rid)
			case cdc.Update:
				if row.DeleteTime != nil {
					origin, ok := t.memory.ObjectRelationTable.GetByID(row.FromObjectID, t.relationTypeID, row.ToObjectID)
					if ok && origin.DeleteTime == nil {
						del = append(del, rid)
						// trigger delete
					}
					continue
				}
				update = append(update, rid)
			case cdc.Delete:
				del = append(del, rid)
			}
		case *model.ObjectRelationMetaValue:
			if row.RelationTypeID != t.relationTypeID {
				continue
			}
			need = append(need, event)
			update = append(update, relationID{
				from: row.FromObjectID,
				to:   row.ToObjectID,
			})
		}
	}
	if len(need) == 0 {
		return
	}
	t.mutex.Lock()
	if !t.loaded {
		t.bufferedEvents = append(t.bufferedEvents, need)
		t.mutex.Unlock()
		return
	}
	t.mutex.Unlock()
	t.memory.OnEvents(need)
	if len(add) == 0 && len(del) == 0 && len(update) == 0 {
		return
	}
	var metaTypes = map[int]*model.ObjectRelationMeta{}
	t.types.Read(func(d *typetables.Database) {
		relationMetas := d.ObjectRelationMetaTable.FilterByRelationTypeID(t.relationTypeID)
		for _, meta := range relationMetas {
			var m = meta.ObjectRelationMeta
			metaTypes[meta.ID] = &m
		}
	})
	for _, id := range add {
		rel, ok := t.memory.ObjectRelationTable.GetByID(id.from, t.relationTypeID, id.to)
		if !ok {
			continue
		}
		relation := t.convert(&rel.ObjectRelation, metaTypes)
		t.mutex.Lock()
		t.relations[id] = relation
		t.mutex.Unlock()

		relation = t.richRelation(relation)
		var evt = storage.RelationEvent{
			Relation: relation,
			Event:    cdc.Create,
		}
		for _, handler := range t.handlers {
			if handler.Filter(relation) {
				handler.OnEvent(evt)
			}
		}
	}
	for _, id := range del {
		t.mutex.Lock()
		rel, ok := t.relations[id]
		if !ok {
			t.mutex.Unlock()
			continue
		}
		delete(t.relations, id)
		t.mutex.Unlock()

		rel = t.richRelation(rel)
		var evt = storage.RelationEvent{
			Relation: rel,
			Event:    cdc.Delete,
		}
		for _, handler := range t.handlers {
			if handler.Filter(rel) {
				handler.OnEvent(evt)
			}
		}
	}
	for _, id := range update {
		rel, ok := t.memory.ObjectRelationTable.GetByID(id.from, t.relationTypeID, id.to)
		if !ok {
			continue
		}
		relation := t.convert(&rel.ObjectRelation, metaTypes)
		t.mutex.Lock()
		t.relations[id] = relation
		t.mutex.Unlock()
		relation = t.richRelation(relation)
		var evt = storage.RelationEvent{
			Relation: relation,
			Event:    cdc.Update,
		}
		for _, handler := range t.handlers {
			if handler.Filter(relation) {
				handler.OnEvent(evt)
			}
		}
	}
}
