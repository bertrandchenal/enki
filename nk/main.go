package main

import (
	"fmt"
	"bitbucket.org/bertrandchenal/enki"
	"github.com/codegangsta/cli"
	"log"
	"os"
	"path"
	"sort"
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

	ANSI_RED = "\x1b[31;1m"
	ANSI_BLUE = "\x1b[34;1m"
	ANSI_GREEN = "\x1b[32;1m"
	ANSI_RESET = "\x1b[0m"
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
		lastState = backend.ReadState(lastState.Timestamp - 1)
	}
}

func showStatus(c *cli.Context) {
	var prefix, color string
	var names []string

	root := c.GlobalString("root")
	backend := getBackend(c)
	defer backend.Close()

	prevState := enki.LastState(backend)
	currentState := enki.NewDirState(root, prevState)

	for name, _ := range currentState.FileStates {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		fstate := currentState.FileStates[name]
		switch {
		case fstate.Status == enki.NEW_FILE:
			prefix = "N"
			color = ANSI_GREEN
		case fstate.Status == enki.CHANGED_FILE:
			prefix = "M"
			color = ANSI_BLUE
		case fstate.Status == enki.DELETED_FILE:
			prefix = "D"
			color = ANSI_RED
		default:
			continue
		}
		fmt.Printf("%v%v%v %v\n", color, prefix, ANSI_RESET, name)
	}
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
		user_time := c.Args()[0]
		loc, _ := time.LoadLocation("Local")
		// Try to interpret the given string
		for _, format := range READ_TIME {
			ts, err = time.ParseInLocation(format, user_time, loc)
			if err == nil {
				break
			}
		}
		if err != nil {
			fmt.Println(err)
			return
		}
		prevState = backend.ReadState(ts.Unix())
		if prevState == nil {
			fmt.Printf("No snapshot found for '%v'\n", user_time)
			return
		}
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
