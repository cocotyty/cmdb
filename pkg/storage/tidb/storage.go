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
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/juju/loggo"
	v1 "github.com/zhihu/cmdb/pkg/api/v1"
	"github.com/zhihu/cmdb/pkg/model"
	"github.com/zhihu/cmdb/pkg/model/typetables"
	"github.com/zhihu/cmdb/pkg/storage"
	"github.com/zhihu/cmdb/pkg/storage/cache"
)

var log = loggo.GetLogger("storage")

var (
	ErrUnknownStatus      = invalidArguments("unknown status")
	ErrUnknownState       = invalidArguments("unknown state")
	ErrUnknownMeta        = invalidArguments("unknown meta")
	ErrNoSuchType         = invalidArguments("no such type")
	ErrVersionMatchFailed = invalidArguments("version match failed")
)

func NewStorage(db *sqlx.DB, tsGetter storage.TimestampGetter, cache *cache.Cache) *Storage {
	return &Storage{db: db, tsGetter: tsGetter, cache: cache}
}

type Storage struct {
	db       *sqlx.DB
	tsGetter storage.TimestampGetter
	cache    *cache.Cache
}

const GetTSTimeout = time.Millisecond * 1000

func (s *Storage) GetTS(ctx context.Context) (ts uint64, err error) {
	timeout, cancelFunc := context.WithTimeout(ctx, GetTSTimeout)
	defer cancelFunc()
	ts, err = s.tsGetter.Get(timeout)
	if err != nil {
		return 0, internalError(err)
	}
	return ts, nil
}

func (s *Storage) getType(obj *v1.Object) (typID, statusID, stateID int, namedMeta map[string]model.ObjectMeta, err error) {
	namedMeta = map[string]model.ObjectMeta{}
	s.cache.TypeCache(func(d *typetables.Database) {
		typ, ok := d.ObjectTypeTable.GetByName(obj.Type)
		if !ok {
			err = ErrNoSuchType
			return
		}

		for _, meta := range typ.ObjectMeta {
			namedMeta[meta.Name] = meta.ObjectMeta
		}
		typID = typ.ID
		status, ok := d.ObjectStatusTable.GetByTypeIDName(typID, obj.Status)
		if !ok {
			err = ErrUnknownStatus
		} else {
			statusID = status.ID
		}
		state, ok := d.ObjectStateTable.GetByStatusIDName(statusID, obj.State)
		if !ok {
			err = ErrUnknownState
		} else {
			stateID = state.ID
		}
	})
	return
}
