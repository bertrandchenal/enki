package main

import (
	"bitbucket.org/bertrandchenal/enki"
	"github.com/codegangsta/cli"
	"log"
	"os"
	"path"
)

const dotEnki = ".nk"

func getBackend(c *cli.Context) enki.Backend {
	dotDir := path.Join(c.GlobalString("root"), dotEnki)
	info, err := os.Stat(dotDir)

	if err == nil {
		if !info.IsDir() {
			log.Print("Abort, unexpected file ", dotDir)
			os.Exit(1)
		}
	} else if os.IsNotExist(err) {
		if !c.GlobalBool("dry-run") {
			os.Mkdir(dotDir, 0750)
		}
		log.Print("Directory '.nk' created")
	} else {
		panic(err)
	}

	return enki.NewBoltBackend(dotDir)
}

func showLogs(c *cli.Context) {
	println("added task: ", c.Args().First())
}

func showStatus(c *cli.Context) {
	println("added task: ", c.Args().First())
}

func restoreSnapshot(c *cli.Context) {
	dry_run := c.GlobalBool("dry-run")
	root := c.GlobalString("root")
	backend := getBackend(c)
	defer backend.Close()
	prevState := enki.LastState(backend)
	currentState := enki.NewDirState(root, prevState)

	if dry_run {
		return // TODO give info
	}
	currentState.RestorePrev(backend)
}

func createSnapshot(c *cli.Context) {
	dry_run := c.GlobalBool("dry-run")
	root := c.GlobalString("root")
	backend := getBackend(c)
	defer backend.Close()
	prevState := enki.LastState(backend)
	currentState := enki.NewDirState(root, prevState)

	if dry_run {
		return // TODO give info
	}
	currentState.Snapshot(backend)
}

func initRepo(c *cli.Context) {
}

func main() {
	app := cli.NewApp()
	app.Name = "enki"
	app.Usage = "data versionning"
	app.EnableBashCompletion = true
	app.Commands = []cli.Command{
		{
			Name: "log",
			Usage: "Show repository logs",
			Flags: []cli.Flag {
				cli.IntFlag{
					Name: "limit, l",
					Usage: "Limit the number of changes to show",
				},
			},
			Action: showLogs,
		},
		{
			Name: "restore",
			Aliases: []string{"re"},
			Usage: "Restore previous snapshot",
			Action: restoreSnapshot,
		},
		{
			Name: "snapshot",
			Aliases: []string{"sn", "snap"},
			Usage: "Create snapshot",
			Action: createSnapshot,
		},
		{
			Name: "status",
			Aliases: []string{"st"},
			Usage: "Show changed files in repository",
			Action: showStatus,
		},
	}
	
	app.Flags = []cli.Flag {
		cli.BoolFlag{
			Name: "dry-run, n",
			Usage: "Dry run",
		},
		cli.StringFlag{
			Name: "root, r",
			Usage: "Root of repository",
			Value: ".",
		},
	}

	app.Run(os.Args)
}
