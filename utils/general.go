package utils

import (
	"fmt"
	"github.com/melbahja/goph"
	"github.com/withmandala/go-log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

var Log = log.New(os.Stderr).WithTimestamp().WithoutColor()

type SSHConnections map[string]*goph.Client

// GetConfigFile manages to get the config file path using 3 different lookups
// The order of search is: SYNCBIT_CONFIG environment variable -> First cli argument -> Input prompt
func GetConfigFile() string {
	// get file path from os environment
	var file = os.Getenv("SYNCBIT_CONFIG")

	// if os env is empty
	if len(file) == 0 {
		// get path from argument
		if len(os.Args) > 1 {
			file = os.Args[1]
		} else {
			// prompt for config file
			fmt.Print("Enter config file name: ")
			fmt.Scanf("%s", &file)

			// if third try fails, send error
			if len(file) == 0 {
				Log.Fatalf("Config file is required")
			}
		}
	}

	return file
}

// GetConfig is used to parse the yaml file and return config struct
func GetConfig() Config {
	var conf Config
	conf.parse(GetConfigFile())
	return conf
}

// GetSSHConnections will connect to SSH from adaptors array and return SSHConnections
func GetSSHConnections(conf Config) SSHConnections {
	conn := make(SSHConnections)
	var wg sync.WaitGroup

	// iterate all the adaptors
	Log.Info("Connecting to adaptors")
	for _, adaptor := range conf.Adaptors {
		wg.Add(1)
		go func(adaptor Adaptor) {
			defer wg.Done()

			// check for file existence
			if f, err := os.Stat(adaptor.Pass); err == nil {
				// if it's directory, stop
				if f.IsDir() {
					Log.Fatalf("%s is a directory. Can't open for SSH Key", adaptor.Pass)
				}

				// create client with ssh key
				Log.Tracef("Establishing connection for %s", adaptor.Name)
				if auth, err := goph.Key(adaptor.Pass, ""); err == nil {
					if cl, err := goph.New(adaptor.User, adaptor.Host, auth); err == nil {
						Log.Tracef("Connected to %s", adaptor.Name)
						conn[adaptor.Name] = cl
					} else {
						// handle ssh connection failure
						Log.Fatal(err.Error())
					}
				}
			} else {
				// create client with password
				Log.Tracef("Establishing connection for %s", adaptor.Name)
				if cl, err := goph.New(adaptor.User, adaptor.Host, goph.Password(adaptor.Pass)); err == nil {
					Log.Tracef("Connected to %s", adaptor.Name)
					conn[adaptor.Name] = cl
				} else {
					// handle ssh connection failure
					Log.Fatal(err.Error())
				}
			}
		}(adaptor)
	}
	wg.Wait()
	return conn
}

// DisconnectSSHConnections will close all the SSH connections
func DisconnectSSHConnections(conn SSHConnections) {
	Log.Info("Closing SSH Connection")
	for n, cl := range conn {
		if err := cl.Close(); err != nil {
			Log.Tracef("Adaptor %s connection didn't close well, will do force close", n)
		} else {
			Log.Tracef("Adaptor %s connection closed", n)
		}
	}
}

// GetAdaptorFromName will give the adaptor details from its name
func GetAdaptorFromName(name string, conf Config) *Adaptor {
	for _, adaptor := range conf.Adaptors {
		if name == adaptor.Name {
			return &adaptor
		}
	}

	return nil
}

// GetStagingFileName will generate a random string of 13 chars and return
func GetStagingFileName() string {
	// init randomizer
	rand.Seed(time.Now().Unix())
	charSet := "abcdedfghijklmnopqrstABCDEFGHIJKLMNOP"

	// make 13 chars long string and return
	var output strings.Builder
	for i := 0; i < 13; i++ {
		random := rand.Intn(len(charSet))
		randomChar := charSet[random]
		output.WriteString(string(randomChar))
	}
	return output.String()
}

// ChunkifyFiles will give the chunks of the files array
func ChunkifyFiles(files []File, limit int) [][]File {
	batches := make([][]File, 0)

	min := func(a, b int) int {
		if a <= b {
			return a
		}
		return b
	}

	for i := 0; i < len(files); i += limit {
		batch := files[i:min(i+limit, len(files))]
		batches = append(batches, batch)
	}

	return batches
}
