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

package cmdbctl

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	v1 "github.com/zhihu/cmdb/pkg/api/v1"
)

func (c *Client) Get(ctx context.Context, name string, query string, format string) error {
	req := &v1.ListObjectRequest{}
	req.Type = name
	req.Query = query
	req.View = v1.ObjectView_NORMAL
	resp, err := c.ObjectsClient.List(ctx, req)
	if err != nil {
		return err
	}
	switch format {
	case YAML:
		return RenderYAML(resp.Objects, true)
	case JSON:
		return RenderJSON(resp.Objects, true)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	_, _ = fmt.Fprintf(w, "NAME\tSTATUS\tSTATE\tMETAS\tDESC\n")
	for _, object := range resp.Objects {
		buf := bytes.NewBuffer(nil)
		for name, value := range object.Metas {
			if buf.Len() > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(name)
			buf.WriteString(": ")
			buf.WriteString(value.Value)
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", object.Name, object.Status, object.State, buf.String(), object.Description)
	}
	_ = w.Flush()
	return err
}
