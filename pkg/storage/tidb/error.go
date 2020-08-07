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
	"github.com/juju/loggo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func internalError(err error) error {
	if err == nil {
		return nil
	}
	log.LogCallf(1, loggo.ERROR, "%s", err)
	return status.New(codes.Internal, "server internal error").Err()
}

func notFound(fmt string, args ...interface{}) error {
	return status.Newf(codes.NotFound, fmt, args...).Err()
}

func unavailable(fmt string, args ...interface{}) error {
	return status.Newf(codes.Unavailable, fmt, args...).Err()
}

func aborted(fmt string, args ...interface{}) error {
	return status.Newf(codes.Aborted, fmt, args...).Err()
}
func invalidArguments(fmt string, args ...interface{}) error {
	return status.Newf(codes.InvalidArgument, fmt, args...).Err()
}

func PathNotFoundError(path string) error {
	return status.Newf(codes.InvalidArgument, "path %s not found", path).Err()
}