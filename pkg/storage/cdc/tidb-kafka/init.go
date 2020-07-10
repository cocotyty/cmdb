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

package tidb_kafka

import (
	"net/url"
	"strings"

	"github.com/zhihu/cmdb/pkg/storage/cdc"
)

const DriverName = "tidb-kafka"

func init() {
	cdc.Register(DriverName, func(source string) (w cdc.Watcher, err error) {
		c := &Consumer{}
		u, err := url.Parse(source)
		if err != nil {
			return nil, err
		}
		var values = u.Query()
		var topic = values.Get("topic")
		var version = values.Get("version")
		c.version = version
		c.topic = topic
		c.addrs = strings.Split(u.Host, ",")
		return c, nil
	})
}
