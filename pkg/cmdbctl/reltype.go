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
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	v1 "github.com/zhihu/cmdb/pkg/api/v1"
)

func (c *Client) GetRelationTypes(ctx context.Context, format string) error {
	req := &v1.ListRelationTypesRequest{}
	resp, err := c.RelationTypesClient.List(ctx, req)
	if err != nil {
		return err
	}
	switch format {
	case YAML:
		return RenderYAML(resp.Types, true)
	case JSON:
		return RenderJSON(resp.Types, true)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	_, _ = fmt.Fprintf(w, "NAME\tMETAS\n")

	for _, object := range resp.Types {
		_, _ = fmt.Fprintf(w, "%s\t%s\n",
			fmt.Sprintf("%s(%s=>%s)", object.Name, object.From, object.To),
			formatMetas(object.Metas))
	}
	_ = w.Flush()
	return err
}
