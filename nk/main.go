package main

import (
	"fmt"
	"bitbucket.org/bertrandchenal/enki"
	"github.com/codegangsta/cli"
	"log"
	"os"
	"path"
	"time"
)

const (
	dotEnki = ".nk"
	FULL_FMT = "2006-01-02T15:04:05"
	YEAR_FMT = "2006"
	MONTH_FMT = "2006-01"
	DAY_FMT = "2006-01-02"
	HOUR_FMT = "2006-01-02T15"
	MIN_FMT = "2006-01-02T15:04"
)


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
	backend := getBackend(c)
	defer backend.Close()
	lastState := enki.LastState(backend)
	for lastState != nil {
		ts := time.Unix(lastState.Timestamp, 0)
		println(ts.Format(FULL_FMT))
		lastState = backend.GetState(lastState.Timestamp - 1)
	}
}

func showStatus(c *cli.Context) {
	println("added task: ", c.Args().First())
}

func restoreSnapshot(c *cli.Context) {
	var prevState *enki.DirState
	var err error
	var ts time.Time
	READ_TIME := [...]string{FULL_FMT, YEAR_FMT, MONTH_FMT, DAY_FMT, HOUR_FMT,
		MIN_FMT}

	root := c.GlobalString("root")
	backend := getBackend(c)
	defer backend.Close()

	if len(c.Args()) > 0 {
		loc, _ := time.LoadLocation("Local")

		// Try to interpret the given string
		for _, format := range READ_TIME {
			ts, err = time.ParseInLocation(format, c.Args()[0], loc)
			if err == nil {
				break
			}
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		prevState = backend.GetState(ts.Unix())
	} else {
		prevState = enki.LastState(backend)
	}

	currentState := enki.NewDirState(root, prevState)
	currentState.RestorePrev(backend)
}

func createSnapshot(c *cli.Context) {
	root := c.GlobalString("root")
	backend := getBackend(c)
	defer backend.Close()


	prevState := enki.LastState(backend)
	currentState := enki.NewDirState(root, prevState)

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
