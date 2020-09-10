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

func (c *Client) GetRelations(ctx context.Context, from, to, relation string, format string) error {
	req := &v1.ListRelationRequest{}
	req.From = from
	req.To = to
	req.Relation = relation
	resp, err := c.RelationsClient.List(ctx, req)
	if err != nil {
		return err
	}
	switch format {
	case YAML:
		return RenderYAML(resp.Relations, true)
	case JSON:
		return RenderJSON(resp.Relations, true)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	_, _ = fmt.Fprintf(w, "NAME\tMETAS\n")

	for _, object := range resp.Relations {
		_, _ = fmt.Fprintf(w, "%s\t%s\n",
			relationFmt(object.Relation, object.From.Type, object.From.Name, object.To.Type, object.To.Name),
			formatMetaValues(object.Metas))
	}
	_ = w.Flush()
	return err
}

func relationFmt(rel, from, fname, to, tname string) string {
	return fmt.Sprintf("%s(%s/%s=>%s/%s)", rel, from, fname, to, tname)
}
