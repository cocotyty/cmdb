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

package ticdc

import (
	"github.com/juju/loggo"
	cmodel "github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec"
	"github.com/zhihu/cmdb/pkg/storage/cdc"
)

var log = loggo.GetLogger("ticdc")

type Consumer struct {
	version      string
	addrs        []string
	topic        string
	transactions map[uint64][]*Event
	cdc.Handlers
}

func (c *Consumer) Consume(key []byte, value []byte) error {
	log.Infof("msg: %s", string(key))
	batchDecoder, err := codec.NewJSONEventBatchDecoder(key, value)
	if err != nil {
		log.Errorf("create decoder: %s", err)
		return err
	}
	for {
		tp, hasNext, err := batchDecoder.HasNext()
		if err != nil {
			log.Errorf("Decoder: %s", err)
			return err
		}
		if !hasNext {
			break
		}
		switch tp {
		case cmodel.MqMessageTypeRow:
			row, err := batchDecoder.NextRowChangedEvent()
			if err != nil {
				log.Errorf("MqMessageTypeRow: %s", err)
				return err
			}
			obj, err := Convert(row)
			events := c.transactions[row.CommitTs]
			var typ cdc.EventType = cdc.Update
			if row.Delete {
				typ = cdc.Delete
			}
			events = append(events, &Event{
				TS:     row.CommitTs,
				Object: obj,
				Type:   typ,
			})
			log.Debugf("%s: %s", typ, obj)
		case cmodel.MqMessageTypeResolved:
			ts, err := batchDecoder.NextResolvedEvent()
			if err != nil {
				log.Infof("MqMessageTypeResolved: %s", err)
				return err
			}
			log.Infof("resolve: %d", ts)
			for _, events := range c.transactions {
				c.handleTransaction(events)
			}
			c.transactions = nil
		}
	}
	return nil
}

func (c *Consumer) handleTransaction(evts []*Event) {
	var events []cdc.Event
	for _, evt := range evts {
		var e = cdc.Event{}
		e.Type = evt.Type
		e.Row = evt.Object
		events = append(events, e)
	}
	log.Infof("transaction: %s", events)
	c.Broadcast(events)
}

type Event struct {
	TS     uint64
	Object interface{}
	Type   cdc.EventType
}
