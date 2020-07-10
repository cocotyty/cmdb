// Copyright 2020 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/zhihu/cmdb/pkg/model/typetables"
	"github.com/zhihu/cmdb/pkg/storage/cdc"
)

type relationTypeKey struct {
	From     string
	To       string
	Relation string
}

func (r relationTypeKey) String() string {
	return r.From + " --" + r.Relation + "-> " + r.To
}

type Cache struct {
	typeCache       *Types
	objectsCaches   sync.Map
	relationsCaches sync.Map
	watcher         cdc.Watcher
	db              *sqlx.DB
}

var defaultInitTimeout = 5 * time.Second

func NewCache(watcher cdc.Watcher, db *sqlx.DB) (*Cache, error) {
	cache := NewTypes()
	watcher.AddEventHandler(cache)
	ctx, cancel := context.WithTimeout(context.Background(), defaultInitTimeout)
	defer cancel()
	err := cache.InitData(ctx, db)
	if err != nil {
		watcher.RemoveEventHandler(cache)
		return nil, err
	}
	err = watcher.Start()
	if err != nil {
		return nil, err
	}
	return &Cache{
		typeCache: cache,
		watcher:   watcher,
		db:        db,
	}, nil
}

func (c *Cache) TypeCache(fn func(d *typetables.Database)) {
	c.typeCache.Read(fn)
}

func (c *Cache) ReloadTypeCache(ctx context.Context, db *sqlx.DB) error {
	return c.typeCache.InitData(ctx, db)
}

type States struct {
	Types         []string
	RelationTypes []relationTypeKey
	Objects       map[string]int
	Relations     map[string]int
}

func (c *Cache) States() States {
	var states States
	states.Relations = map[string]int{}
	states.Objects = map[string]int{}
	c.relationsCaches.Range(func(key, value interface{}) bool {
		states.Relations[key.(relationTypeKey).String()] = value.(*relationsHolder).Size()
		return true
	})
	c.objectsCaches.Range(func(key, value interface{}) bool {
		states.Objects[key.(string)] = value.(*objectsHolder).Size()
		return true
	})
	c.typeCache.Read(func(d *typetables.Database) {
		states.Types = make([]string, 0, len(d.ObjectTypeTable.Name))
		for name, _ := range d.ObjectTypeTable.Name {
			states.Types = append(states.Types, name.Name)
		}
		states.RelationTypes = make([]relationTypeKey, 0, len(d.ObjectRelationTypeTable.LogicalID))
		for id := range d.ObjectRelationTypeTable.LogicalID {
			var toName, fromName string
			from, ok := d.ObjectTypeTable.GetByID(id.FromTypeID)
			if !ok {
				fromName = "unknown"
			} else {
				fromName = from.Name
			}
			to, ok := d.ObjectTypeTable.GetByID(id.ToTypeID)
			if !ok {
				toName = "unknown"
			} else {
				toName = to.Name
			}
			states.RelationTypes = append(states.RelationTypes, relationTypeKey{
				From:     fromName,
				To:       toName,
				Relation: id.Name,
			})
		}
	})
	return states
}

func (c *Cache) GetObjects(ctx context.Context, name string) (*Objects, error) {
	var finalizer = func() {
		c.objectsCaches.Delete(name)
	}

	if loaded, ok := c.objectsCaches.Load(name); ok {
		return loaded.(*objectsHolder).Get(ctx, finalizer)
	}
	var id int
	c.typeCache.Read(func(d *typetables.Database) {
		var obj, ok = d.ObjectTypeTable.GetByName(name)
		if ok {
			id = obj.ID
		}
	})
	if id == 0 {
		return nil, errors.New("no such type")
	}
	i := &objectsHolder{
		typeCache: c.typeCache,
		id:        id,
		name:      name,
		watcher:   c.watcher,
		db:        c.db,
	}
	act, _ := c.objectsCaches.LoadOrStore(name, i)
	return act.(*objectsHolder).Get(ctx, finalizer)
}

func (c *Cache) GetRelations(ctx context.Context, from string, to string, relation string) (*Relations, error) {
	var key = relationTypeKey{
		From:     from,
		To:       to,
		Relation: relation,
	}
	var finalizer = func() {
		c.objectsCaches.Delete(key)
	}

	if loaded, ok := c.relationsCaches.Load(key); ok {
		return loaded.(*relationsHolder).Get(ctx, finalizer)
	}
	var id int
	c.typeCache.Read(func(d *typetables.Database) {
		fromType, ok := d.ObjectTypeTable.GetByName(from)
		if !ok {
			return
		}
		toType, ok := d.ObjectTypeTable.GetByName(to)
		if !ok {
			return
		}
		rel, ok := d.ObjectRelationTypeTable.GetByLogicalID(fromType.ID, toType.ID, relation)
		if ok {
			id = rel.ID
		}
	})
	if id == 0 {
		return nil, errors.New("no such type")
	}
	holder := &relationsHolder{
		id:           id,
		relationType: key,
		cache:        c,
	}
	act, _ := c.objectsCaches.LoadOrStore(key, holder)
	return act.(*relationsHolder).Get(ctx, finalizer)
}
