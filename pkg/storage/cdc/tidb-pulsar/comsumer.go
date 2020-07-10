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

package tidb_pulsar

import (
	"errors"
	"net/url"
	"os"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zhihu/cmdb/pkg/storage/cdc/ticdc"
)

var nodename, _ = os.Hostname()

func NewConsumer(link string) (c *Consumer, err error) {
	u, err := url.Parse(link)
	if err != nil {
		return nil, err
	}
	options, err := parseOptions(u)
	if err != nil {
		return nil, err
	}

	var topic = strings.Trim(u.Path, "/")
	if topic == "" {
		return nil, errors.New("no topic provide")
	}
	client, err := pulsar.NewClient(*options)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		client: client,
		topic:  topic,
	}, nil

}

type Consumer struct {
	topic    string
	client   pulsar.Client
	consumer pulsar.Consumer
	ticdc.Consumer
}

func (c *Consumer) Start() error {
	consumer, err := c.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            c.topic,
		SubscriptionName: "cmdb_" + nodename,
		Type:             pulsar.Exclusive,
	})
	if err != nil {
		return err
	}
	c.consumer = consumer
	go c.receive()
	return nil
}

func (c *Consumer) receive() {
	messages := c.consumer.Chan()
	for msg := range messages {
		_ = c.Consumer.Consume([]byte(msg.Key()), msg.Payload())
	}
}

func (c *Consumer) Close() error {
	c.consumer.Close()
	c.client.Close()
	return nil
}
