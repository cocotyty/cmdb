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
)

type relationsHolder struct {
	id           int
	relationType relationTypeKey
	cache        *Cache
	mutex        sync.Mutex
	inited       bool
	initErr      error
	relations    *Relations
}

func (i *relationsHolder) Size() int {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if i.initErr != nil {
		return -1
	}
	if !i.inited {
		return -1
	}
	return i.relations.Size()
}

func (i *relationsHolder) Get(ctx context.Context, clear func()) (o *Relations, err error) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.initErr != nil {
		return nil, i.initErr
	}
	if i.inited {
		return i.relations, nil
	}
	var from *Objects
	var to *Objects
	var finalizer = func() {
		clear()
		if from != nil {
			from.UnRef()
		}
		if to != nil {
			to.UnRef()
		}
	}
	from, err = i.cache.GetObjects(ctx, i.relationType.From)
	if err != nil {
		i.initErr = err
		return nil, err
	}
	from.Ref()

	to, err = i.cache.GetObjects(ctx, i.relationType.From)
	if err != nil {
		i.initErr = err
		return nil, err
	}
	to.Ref()

	relations := NewRelations(from, to, i.relationType.Relation, i.id, i.cache.typeCache, finalizer)
	i.cache.watcher.AddEventHandler(relations)
	err = relations.LoadData(ctx, i.cache.db)
	if err != nil {
		finalizer()
		i.cache.watcher.RemoveEventHandler(relations)
		relations.ResetBuffer()
		return nil, err
	}
	relations.StartGC()
	i.inited = true
	i.relations = relations
	return i.relations, nil
}
