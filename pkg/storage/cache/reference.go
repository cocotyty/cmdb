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
	"sync"
	"time"
)

func NewReferenceManager(trigger func(), waitTimeout time.Duration, delay time.Duration) *ReferenceManager {
	r := &ReferenceManager{
		waitTimeout: waitTimeout,
		delay:       delay,
		trigger:     trigger,
	}
	r.init()
	return r
}

type ReferenceManager struct {
	reference   int
	waitTimeout time.Duration
	delay       time.Duration
	trigger     func()
	stopWait    chan struct{}
	markCancel  bool
	cancel      chan struct{}
	locker      sync.Mutex
	released    bool
}

func (r *ReferenceManager) init() {
	r.stopWait = make(chan struct{}, 1)
	go r.autoRelease()
}

func (r *ReferenceManager) Ref() bool {
	r.locker.Lock()
	if r.released {
		r.locker.Unlock()
		return false
	}
	r.cancelRelease()
	r.reference++
	r.locker.Unlock()
	return true
}

func (r *ReferenceManager) UnRef() {
	r.locker.Lock()
	r.reference--
	if r.reference == 0 {
		r.tryRelease()
	}
	r.locker.Unlock()
}

func (r *ReferenceManager) cancelRelease() {
	if r.cancel == nil {
		return
	}
	r.markCancel = true
	select {
	case r.cancel <- struct{}{}:
	default:
	}
}

func (r *ReferenceManager) RefSize() int {
	r.locker.Lock()
	defer r.locker.Unlock()
	return r.reference
}

func (r *ReferenceManager) Released() bool {
	r.locker.Lock()
	defer r.locker.Unlock()
	return r.released
}

func (r *ReferenceManager) tryRelease() {
	if r.released {
		return
	}
	if r.cancel != nil {
		return
	}
	var cancel = make(chan struct{})
	r.markCancel = false
	r.cancel = cancel
	go r.waitRelease(cancel)
}

func (r *ReferenceManager) waitRelease(cancel chan struct{}) {
	var timer = time.NewTimer(r.delay)
	select {
	case <-timer.C:
		r.locker.Lock()
		r.cancel = nil
		if r.markCancel {
			r.locker.Unlock()
			return
		}
		r.released = true
		r.trigger()
		r.locker.Unlock()
	case <-cancel:
		if !timer.Stop() {
			<-timer.C
		}
		r.locker.Lock()
		r.cancel = nil
		r.locker.Unlock()
		return
	}
}

func (r *ReferenceManager) autoRelease() {
	timer := time.NewTimer(r.waitTimeout)
	select {
	case <-timer.C:
		r.locker.Lock()
		if r.reference == 0 {
			r.tryRelease()
		}
		r.locker.Unlock()
		return
	case <-r.stopWait:
		if !timer.Stop() {
			<-timer.C
		}
		return
	}
}
