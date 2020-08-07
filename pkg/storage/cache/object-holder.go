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

	"github.com/jmoiron/sqlx"
	"github.com/zhihu/cmdb/pkg/storage/cdc"
)

type objectsHolder struct {
	typeCache *Types
	id        int
	name      string
	mutex     sync.Mutex
	inited    bool
	initErr   error
	watcher   cdc.Watcher
	objects   *Objects
	db        *sqlx.DB
	c         *Cache
}

func (i *objectsHolder) Size() int {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if i.initErr != nil {
		return -1
	}
	if !i.inited {
		return -1
	}
	return i.objects.Size()
}

func (i *objectsHolder) Get(ctx context.Context, finalizer func()) (o *Objects, err error) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if i.initErr != nil {
		return nil, i.initErr
	}
	if i.inited {
		return i.objects, nil
	}
	objects := NewObjects(i.typeCache, i.id, i.name, finalizer)
	i.watcher.AddEventHandler(objects)
	err = objects.LoadData(ctx, i.db)
	if err != nil {
		finalizer()
		i.watcher.RemoveEventHandler(objects)
		objects.ResetBuffer()
		return nil, err
	}
	objects.StartGC()
	i.inited = true
	i.objects = objects
	return i.objects, nil
}
