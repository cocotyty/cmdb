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

// DO NOT EDIT IT.

package relations

import (
	cdc "github.com/zhihu/cmdb/pkg/storage/cdc"

	model "github.com/zhihu/cmdb/pkg/model"
)

func noop(_ interface{}) {}

type RichObjectRelation struct {
	model.ObjectRelation
}

type ObjectRelationTable struct {
	ID map[IndexObjectRelationID]*RichObjectRelation
}

func (t *ObjectRelationTable) Init() {
	t.ID = map[IndexObjectRelationID]*RichObjectRelation{}

}

type IndexObjectRelationID struct {
	FromObjectID   int
	RelationTypeID int
	ToObjectID     int
}

func (t *ObjectRelationTable) GetByID(FromObjectID int, RelationTypeID int, ToObjectID int) (row *RichObjectRelation, ok bool) {
	row, ok = t.ID[IndexObjectRelationID{FromObjectID: FromObjectID, RelationTypeID: RelationTypeID, ToObjectID: ToObjectID}]
	return
}

type RichObjectRelationMetaValue struct {
	model.ObjectRelationMetaValue
}

type ObjectRelationMetaValueTable struct {
	ID         map[IndexObjectRelationMetaValueID]*RichObjectRelationMetaValue
	RelationID map[IndexObjectRelationMetaValueRelationID][]*RichObjectRelationMetaValue
}

func (t *ObjectRelationMetaValueTable) Init() {
	t.ID = map[IndexObjectRelationMetaValueID]*RichObjectRelationMetaValue{}
	t.RelationID = map[IndexObjectRelationMetaValueRelationID][]*RichObjectRelationMetaValue{}

}

type IndexObjectRelationMetaValueID struct {
	FromObjectID   int
	RelationTypeID int
	ToObjectID     int
	MetaID         int
}

type IndexObjectRelationMetaValueRelationID struct {
	FromObjectID   int
	RelationTypeID int
	ToObjectID     int
}

func (t *ObjectRelationMetaValueTable) GetByID(FromObjectID int, RelationTypeID int, ToObjectID int, MetaID int) (row *RichObjectRelationMetaValue, ok bool) {
	row, ok = t.ID[IndexObjectRelationMetaValueID{FromObjectID: FromObjectID, RelationTypeID: RelationTypeID, ToObjectID: ToObjectID, MetaID: MetaID}]
	return
}
func (t *ObjectRelationMetaValueTable) FilterByRelationID(FromObjectID int, RelationTypeID int, ToObjectID int) (rows []*RichObjectRelationMetaValue) {
	rows, _ = t.RelationID[IndexObjectRelationMetaValueRelationID{FromObjectID: FromObjectID, RelationTypeID: RelationTypeID, ToObjectID: ToObjectID}]
	return
}

type Database struct {
	ObjectRelationTable ObjectRelationTable

	ObjectRelationMetaValueTable ObjectRelationMetaValueTable
}

func (d *Database) Init() {
	d.ObjectRelationTable.Init()
	d.ObjectRelationMetaValueTable.Init()
}

func (d *Database) InsertObjectRelation(row *model.ObjectRelation) (ok bool) {
	if row.DeleteTime != nil {
		return false
	}
	var richRow = &RichObjectRelation{
		ObjectRelation: *row,
	}

	{
		var index = IndexObjectRelationID{row.FromObjectID, row.RelationTypeID, row.ToObjectID}
		_, ok := d.ObjectRelationTable.ID[index]
		if ok {
			return false
		}
		d.ObjectRelationTable.ID[index] = richRow

	}

	return true
}

func (d *Database) UpdateObjectRelation(row *model.ObjectRelation) (ok bool) {
	if row.DeleteTime != nil {
		return d.DeleteObjectRelation(row)
	}
	var index = IndexObjectRelationID{row.FromObjectID, row.RelationTypeID, row.ToObjectID}
	origin, ok := d.ObjectRelationTable.ID[index]
	if !ok {
		return d.InsertObjectRelation(row)
	}
	origin.ObjectRelation = *row
	return true
}

func (d *Database) DeleteObjectRelation(row *model.ObjectRelation) (ok bool) {
	var index = IndexObjectRelationID{row.FromObjectID, row.RelationTypeID, row.ToObjectID}
	richRow, ok := d.ObjectRelationTable.ID[index]
	if !ok {
		return false
	}
	noop(richRow)

	{
		var index = IndexObjectRelationID{row.FromObjectID, row.RelationTypeID, row.ToObjectID}
		delete(d.ObjectRelationTable.ID, index)

	}

	return true
}
func (d *Database) InsertObjectRelationMetaValue(row *model.ObjectRelationMetaValue) (ok bool) {
	if row.DeleteTime != nil {
		return false
	}
	var richRow = &RichObjectRelationMetaValue{
		ObjectRelationMetaValue: *row,
	}

	{
		var index = IndexObjectRelationMetaValueID{row.FromObjectID, row.RelationTypeID, row.ToObjectID, row.MetaID}
		_, ok := d.ObjectRelationMetaValueTable.ID[index]
		if ok {
			return false
		}
		d.ObjectRelationMetaValueTable.ID[index] = richRow

	}

	{
		var index = IndexObjectRelationMetaValueRelationID{row.FromObjectID, row.RelationTypeID, row.ToObjectID}

		list := d.ObjectRelationMetaValueTable.RelationID[index]
		list = append(list, richRow)
		d.ObjectRelationMetaValueTable.RelationID[index] = list
	}

	return true
}

func (d *Database) UpdateObjectRelationMetaValue(row *model.ObjectRelationMetaValue) (ok bool) {
	if row.DeleteTime != nil {
		return d.DeleteObjectRelationMetaValue(row)
	}
	var index = IndexObjectRelationMetaValueID{row.FromObjectID, row.RelationTypeID, row.ToObjectID, row.MetaID}
	origin, ok := d.ObjectRelationMetaValueTable.ID[index]
	if !ok {
		return d.InsertObjectRelationMetaValue(row)
	}
	origin.ObjectRelationMetaValue = *row
	return true
}

func (d *Database) DeleteObjectRelationMetaValue(row *model.ObjectRelationMetaValue) (ok bool) {
	var index = IndexObjectRelationMetaValueID{row.FromObjectID, row.RelationTypeID, row.ToObjectID, row.MetaID}
	richRow, ok := d.ObjectRelationMetaValueTable.ID[index]
	if !ok {
		return false
	}
	noop(richRow)

	{
		var index = IndexObjectRelationMetaValueID{row.FromObjectID, row.RelationTypeID, row.ToObjectID, row.MetaID}
		delete(d.ObjectRelationMetaValueTable.ID, index)

	}

	{
		var index = IndexObjectRelationMetaValueRelationID{row.FromObjectID, row.RelationTypeID, row.ToObjectID}

		list := d.ObjectRelationMetaValueTable.RelationID[index]
		var newList = make([]*RichObjectRelationMetaValue, 0, len(list)-1)
		for _, item := range list {
			if item.ObjectRelationMetaValue.FromObjectID != row.FromObjectID || item.ObjectRelationMetaValue.RelationTypeID != row.RelationTypeID || item.ObjectRelationMetaValue.ToObjectID != row.ToObjectID || item.ObjectRelationMetaValue.MetaID != row.MetaID {
				newList = append(newList, item)
			}
		}
		d.ObjectRelationMetaValueTable.RelationID[index] = newList
	}

	return true
}

func (d *Database) OnEvents(transaction []cdc.Event) {
	for _, event := range transaction {
		switch row := event.Row.(type) {
		case *model.ObjectRelation:
			switch event.Type {
			case cdc.Create:
				d.InsertObjectRelation(row)
			case cdc.Update:
				d.UpdateObjectRelation(row)
			case cdc.Delete:
				d.DeleteObjectRelation(row)
			}
		case *model.ObjectRelationMetaValue:
			switch event.Type {
			case cdc.Create:
				d.InsertObjectRelationMetaValue(row)
			case cdc.Update:
				d.UpdateObjectRelationMetaValue(row)
			case cdc.Delete:
				d.DeleteObjectRelationMetaValue(row)
			}
		}
	}
}
