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

package main

import (
	"context"
	"errors"
	"io/ioutil"
	"os"

	"github.com/juju/loggo"
	"github.com/urfave/cli/v2"
	"github.com/zhihu/cmdb/pkg/cmdbctl"
	"github.com/zhihu/cmdb/pkg/signals"
)

var Version = "dev"

var log = loggo.GetLogger("")

const FlagServer = "server"

var globalClient *cmdbctl.Client

func main() {
	ctx := signals.SignalHandler(context.Background())
	app := cli.NewApp()
	app.Name = "cmdbctl"
	app.Version = Version
	app.Usage = "cmdbctl"
	app.Description = `cmdbctl. See https://github.com/zhihu/cmdb for details`
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     FlagServer,
			Aliases:  []string{"s"},
			EnvVars:  []string{"CMDB_SERVER"},
			Value:    "127.0.0.1:8080",
			Required: false,
		},
	}
	app.Before = func(c *cli.Context) (err error) {
		globalClient, err = cmdbctl.NewClient(c.Context, c.String(FlagServer))
		return
	}

	app.Commands = []*cli.Command{
		watch,
		get,
		getType,
		apply,
		getRel,
		getRelType,
	}

	err := app.RunContext(ctx, os.Args)
	if err != nil {
		log.Errorf("application run error: %s", err)
		os.Exit(1)
	}
}

var watch = &cli.Command{
	Name:    "watch",
	Aliases: []string{"w"},
	Action: func(c *cli.Context) error {
		if c.Args().Len() == 0 {
			return errors.New("no type name")
		}
		return globalClient.Watch(c.Context, c.Args().Get(0))
	},
}

var get = &cli.Command{
	Name:    "get",
	Aliases: []string{"g"},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "filter",
			Aliases: []string{"f"},
		},
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
		},
		&cli.StringFlag{
			Name:    "type",
			Aliases: []string{"t"},
		},
	},
	Action: func(c *cli.Context) error {
		var typ = c.String("type")
		var filter = c.String("filter")
		var o = c.String("output")
		return globalClient.Get(c.Context, typ, filter, o)
	},
}

var getRel = &cli.Command{
	Name:    "get-rel",
	Aliases: []string{"gr"},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
		},
		&cli.StringFlag{
			Name:    "relation",
			Aliases: []string{"r"},
		},
		&cli.StringFlag{
			Name:    "from",
			Aliases: []string{"f"},
		},
		&cli.StringFlag{
			Name:    "to",
			Aliases: []string{"t"},
		},
	},
	Action: func(c *cli.Context) error {
		var rel = c.String("relation")
		var from = c.String("from")
		var to = c.String("to")
		var o = c.String("output")
		return globalClient.GetRelations(c.Context, from, to, rel, o)
	},
}

var apply = &cli.Command{
	Name:    "apply",
	Aliases: []string{"a"},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "file",
			Aliases: []string{"f"},
		},
	},
	Action: func(c *cli.Context) error {
		f := c.String("file")
		data, err := ioutil.ReadFile(f)
		if err != nil {
			return err
		}
		return globalClient.Apply(c.Context, string(data))
	},
}

var getRelType = &cli.Command{
	Name:    "get-rel-type",
	Aliases: []string{"grt"},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
		},
	},
	Action: func(c *cli.Context) error {
		var o = c.String("output")
		return globalClient.GetRelationTypes(c.Context, o)
	},
}

var getType = &cli.Command{
	Name:    "get-type",
	Aliases: []string{"gt"},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
		},
		&cli.StringFlag{Name: "name", Aliases: []string{"n"}},
	},
	Action: func(c *cli.Context) error {
		var o = c.String("output")
		var n = c.String("name")
		return globalClient.GetType(c.Context, n, o)
	},
}
