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

package v1

import "strings"

func (x *ObjectType) Equals(r *ObjectType) bool {
	if x.Name != r.Name ||
		x.Description != r.Description {
		return false
	}
	for _, meta := range x.Metas {
		m := r.Metas[meta.Name]
		if m == nil {
			return false
		}
		if m.ValueType != meta.ValueType {
			return false
		}
	}
	for _, meta := range r.Metas {
		m := x.Metas[meta.Name]
		if m == nil {
			return false
		}
	}
	for _, status := range x.Statuses {
		s := r.Statuses[status.Name]
		if s == nil {
			return false
		}
		if s.Description != status.Description {
			return false
		}
		for _, state := range status.States {
			ss := s.States[state.Name]
			if ss == nil {
				return false
			}
			if ss.Description != state.Description {
				return false
			}
		}
		for _, state := range s.States {
			ss := status.States[state.Name]
			if ss == nil {
				return false
			}
		}
	}
	for _, status := range r.Statuses {
		s := x.Statuses[status.Name]
		if s == nil {
			return false
		}
	}
	return true
}

func (x *Object) Equals(r *Object) bool {
	if x.Name != r.Name ||
		x.Description != r.Description ||
		x.Type != r.Type ||
		x.Status != r.Status ||
		x.State != r.State {
		return false
	}
	for name, meta := range x.Metas {
		m := r.Metas[name]
		if m == nil {
			return false
		}
		if m.Value != meta.Value {
			return false
		}
	}
	for name, _ := range r.Metas {
		m := x.Metas[name]
		if m == nil {
			return false
		}
	}
	return true
}

func (x *Relation) Equals(r *Relation) bool {
	if x.Relation != r.Relation ||
		x.From.Type != r.From.Type ||
		x.From.Name != r.From.Name ||
		x.To.Type != r.To.Type ||
		x.To.Name != r.To.Name {
		return false
	}
	for name, meta := range x.Metas {
		m := r.Metas[name]
		if m == nil {
			return false
		}
		if m.Value != meta.Value {
			return false
		}
	}
	for name, _ := range r.Metas {
		m := x.Metas[name]
		if m == nil {
			return false
		}
	}
	return true
}

func (x *RelationType) Equals(r *RelationType) bool {
	if x.Name != r.Name ||
		x.From != r.From ||
		x.To != r.To {
		return false
	}
	for name, meta := range x.Metas {
		m := r.Metas[name]
		if m == nil {
			return false
		}
		if m.ValueType != meta.ValueType || m.Description != meta.Description {
			return false
		}
	}
	for name, _ := range r.Metas {
		m := x.Metas[name]
		if m == nil {
			return false
		}
	}
	return true
}

func CheckObjectType(o *ObjectType) *ObjectType {
	var metas = make(map[string]*ObjectMeta, len(o.Metas))
	o.Name = strings.ToLower(o.Name)
	for name, meta := range o.Metas {
		meta.Name = strings.ToLower(name)
		metas[meta.Name] = meta
	}
	o.Metas = metas
	var statuses = make(map[string]*ObjectStatus, len(o.Statuses))
	for name, status := range o.Statuses {
		status.Name = strings.ToUpper(name)
		var states = make(map[string]*ObjectState, len(status.States))
		for name, state := range status.States {
			state.Name = strings.ToUpper(name)
			states[state.Name] = state
		}
		status.States = states
		statuses[status.Name] = status
	}
	o.Statuses = statuses
	return o
}

func CheckObject(o *Object) *Object {
	o.Type = strings.ToLower(o.Type)
	o.State = strings.ToUpper(o.State)
	o.Status = strings.ToUpper(o.Status)
	var metas = make(map[string]*ObjectMetaValue, len(o.Metas))
	for name, value := range o.Metas {
		metas[strings.ToLower(name)] = value
	}
	o.Metas = metas
	return o
}

func CheckRelation(o *Relation) *Relation {
	o.Relation = strings.ToLower(o.Relation)
	var metas = make(map[string]*ObjectMetaValue, len(o.Metas))
	for name, value := range o.Metas {
		metas[strings.ToLower(name)] = value
	}
	o.Metas = metas
	return o
}

func CheckRelationType(o *RelationType) *RelationType {
	o.Name = strings.ToLower(o.Name)
	o.From = strings.ToLower(o.From)
	o.To = strings.ToLower(o.To)
	var metas = make(map[string]*ObjectMeta, len(o.Metas))
	for name, value := range o.Metas {
		name = strings.ToLower(name)
		value.Name = name
		metas[name] = value
	}
	o.Metas = metas
	return o
}
