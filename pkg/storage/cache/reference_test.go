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
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestReferenceManager(t *testing.T) {
	wait := make(chan struct{}, 1)
	trigger := func() {
		wait <- struct{}{}
	}
	waitTimeout := 10 * time.Millisecond
	delay := 20 * time.Millisecond
	// test auto release when no ref
	var c *ReferenceManager
	newG, usedTime := stats(func() {
		c = NewReferenceManager(trigger, waitTimeout, delay)
		<-wait
	})

	if !c.Released() {
		t.Fatal("except manager released")
	}
	if usedTime.Milliseconds() < 30 || usedTime.Milliseconds() > 40 {
		t.Fatalf("except manager release after 30ms but got :%d", usedTime.Milliseconds())
	}
	if newG > 0 {
		t.Fatalf("except release all goroutine but still alive: %d", newG)
	}
	// test auto release when no ref
	newG, usedTime = stats(func() {
		c = NewReferenceManager(trigger, waitTimeout, delay)
		time.Sleep(time.Millisecond * 2)
		c.Ref()
		if c.RefSize() != 1 {
			t.Fatal("refsize must be 1")
		}
		time.Sleep(time.Millisecond * 40)
		c.UnRef()
		select {
		case <-wait:
			t.Fatalf("should delay")
		default:
		}
		<-wait
	})
	if newG > 0 {
		t.Fatalf("except release all goroutine but still alive: %d", newG)
	}
	if !c.Released() {
		t.Fatal("except manager released")
	}
	if usedTime.Milliseconds() < 62 || usedTime.Milliseconds() > 72 {
		t.Fatalf("except manager release after 72ms but got :%d", usedTime.Milliseconds())
	}

	var count int64 = 0
	c = NewReferenceManager(trigger, waitTimeout, time.Second*10)
	for i := 0; i < 1000; i++ {
		go func() {
			defer atomic.AddInt64(&count, 1)
			if !c.Ref() {
				t.Log("c released")
				return
			}
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
			c.UnRef()
		}()
	}
	<-wait
	t.Log(count, c.Released(), c.RefSize())
}

func stats(fn func()) (newG int, usedTime time.Duration) {
	currentGoroutine := runtime.NumGoroutine()
	start := time.Now()
	fn()
	newG = currentGoroutine - runtime.NumGoroutine()
	usedTime = time.Now().Sub(start)
	return
}
