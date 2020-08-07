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
	"io"
	"os"

	"github.com/Shopify/sarama"
	"github.com/zhihu/cmdb/pkg/storage/cdc/ticdc"
	"github.com/zhihu/cmdb/pkg/tools/kafka"
)

var nodename, _ = os.Hostname()

type Consumer struct {
	closer  io.Closer
	version string
	addrs   []string
	topic   string
	ticdc.Consumer
}

func (c *Consumer) Start() error {
	closer, err := kafka.StartGroupConsume(kafka.Config{
		Addr:     c.addrs,
		Group:    "cmdb_consume_" + nodename,
		Topic:    []string{c.topic},
		Version:  c.version,
		ClientID: "cmdb_consume_" + nodename,
	}, func(msg *sarama.ConsumerMessage) {
		_ = c.Consumer.Consume(msg.Key, msg.Value)
	})
	if err != nil {
		return err
	}
	c.closer = closer
	return nil
}

func (c *Consumer) Close() error {
	return c.closer.Close()
}
