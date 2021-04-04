package utils

import (
	"github.com/goccy/go-yaml"
	"io/ioutil"
	"strings"
)

type Settings struct {
	// Verbose will tell logger to show debug logs on stderr
	Verbose bool `yaml:"verbose"`
	// Colors will add the beautiful distinguishable logs on the stderr
	Colors bool `yaml:"colors"`
}

type Adaptor struct {
	// Name of the adaptor for reference
	Name string `yaml:"name"`
	// User to connect to SSH (default: "root")
	User string `yaml:"user"`
	// Host can be ip or hostname to connect (default: "localhost")
	Host string `yaml:"host"`
	// Pass is used for authenticating the SSH connection (default: ""). It can be either ssh private key or an actual password
	Pass string `yaml:"pass"`
	// Port is the SSH port to create new connection on (default: 22)
	Port int `yaml:"port"`
}

type Hooks struct {
	// PreBackup will be executed before zipping the directory on the source adaptor
	PreBackup []string `yaml:"pre-backup"`
	// PostBackup will be executed after zipping the directory on the source adaptor
	PostBackup []string `yaml:"post-backup"`
	// PreDownload will be executed before downloading the zipped file on the source adaptor
	PreDownload []string `yaml:"pre-download"`
	// PostDownload will be executed after downloading the zipped file on the source adaptor
	PostDownload []string `yaml:"post-download"`
	// PreUpload will be executed before uploading the zip file on the destination adaptor
	PreUpload []string `yaml:"pre-upload"`
	// PostUpload will be executed after uploading the zip file on the destination adaptor
	PostUpload []string `yaml:"post-upload"`
	// PreRestore will be executed before unzipping the file on the destination adaptor
	PreRestore []string `yaml:"pre-restore"`
	// PostRestore will be executed after uploading the file on the destination adaptor
	PostRestore []string `yaml:"post-restore"`
}

type Global struct {
	// Hooks are the actions to be performed on SSH session on particular event
	Hooks Hooks `yaml:"hooks"`
}

type Src struct {
	// Path is the directory for backup
	Path string `yaml:"path"`
	// Adaptor is the name of the connection adaptor from adaptors array
	Adaptor string `yaml:"adaptor"`
	// PreBackup will be executed before zipping the directory
	PreBackup []string `yaml:"pre-backup"`
	// PostBackup will be executed after zipping the directory
	PostBackup []string `yaml:"post-backup"`
	// PreDownload will be executed before downloading the zipped file
	PreDownload []string `yaml:"pre-download"`
	// PostDownload will be executed after downloading the zipped file
	PostDownload []string `yaml:"post-download"`
}

type Dest struct {
	// Path is the directory to restore
	Path string `yaml:"path"`
	// Adaptor is the name of the connection adaptor from adaptors array
	Adaptor string `yaml:"adaptor"`
	// PreUpload will be executed before uploading the zip file
	PreUpload []string `yaml:"pre-upload"`
	// PostUpload will be executed after uploading the zip file
	PostUpload []string `yaml:"post-upload"`
	// PreRestore will be executed before unzipping the file
	PreRestore []string `yaml:"pre-restore"`
	// PostRestore will be executed after uploading the file
	PostRestore []string `yaml:"post-restore"`
}

type File struct {
	// Src config contains the details for the backup
	Src Src `yaml:"src"`

	// Dest config contains the details for the restore
	Dest Dest `yaml:"dest"`
}

// Config struct holds the parsed data for of config file
type Config struct {
	// Settings is used to define the behaviour of the application
	Settings Settings `yaml:"settings"`

	// Adaptors are used to hold the connection objects. This can be used to transfer data
	Adaptors []Adaptor `yaml:"adaptors"`

	// Global scoped actions
	Global Global `yaml:"global"`

	// Files are the directories to zip, download and upload
	Files []File `yaml:"files"`
}

// parse is used to read config and unmarshal its contents
func (c *Config) parse(file string) {
	if raw, err := ioutil.ReadFile(file); err == nil {
		Log.Infof("Parsing %s", file)

		if err := yaml.Unmarshal(raw, c); err != nil {
			Log.Fatalf(err.Error())
		}
		c.validate(file)
	} else {
		Log.Fatal(err.Error())
	}
}

// validate function is used to validate the user input and add defaults
func (c *Config) validate(file string) {
	// enable coloured logging
	if c.Settings.Colors {
		Log.WithColor()
	}

	// enable debug logging
	if c.Settings.Verbose {
		Log.WithDebug()
	}
	Log.Debug("Validating the config file")

	// exit when no adaptors are found
	if len(c.Adaptors) == 0 {
		Log.Fatal("Couldn't find any adaptor to connect to")
	}

	// adding defaults to the adaptor fields
	for _, adaptor := range c.Adaptors {
		if len(adaptor.User) == 0 {
			adaptor.User = "root"
			Log.Debugf("Defaulting user 'root' for %s adaptor", adaptor.Name)
		}

		if len(adaptor.Host) == 0 {
			adaptor.Host = "localhost"
			Log.Debugf("Defaulting host 'localhost' for %s adaptor", adaptor.Name)
		}

		if adaptor.Port == 0 {
			adaptor.Port = 22
			Log.Debugf("Defaulting port '22' for %s adaptor", adaptor.Name)
		}
	}

	// exit when there is no file to transfer
	if len(c.Files) == 0 {
		Log.Fatalf("No files found to transfer")
	}

	// validate adaptor name, paths and fix path trailing /
	for _, file := range c.Files {
		if !c._isValidAdaptor(file.Src.Adaptor) {
			Log.Fatalf("adaptor name %s is not recognized in files", file.Src.Adaptor)
		}

		if !c._isValidAdaptor(file.Dest.Adaptor) {
			Log.Fatalf("adaptor name %s is not recognized in files", file.Src.Adaptor)
		}

		if file.Src.Path == "" {
			Log.Fatal("Source path is missing")
		}

		if strings.HasSuffix(file.Src.Path, "/") {
			file.Src.Path = file.Src.Path[:len(file.Src.Path)-1]
		}

		if file.Dest.Path == "" {
			Log.Fatal("Destination path is missing")
		}

		if strings.HasSuffix(file.Dest.Path, "/") {
			file.Dest.Path = file.Dest.Path[:len(file.Dest.Path)-1]
		}

	}
}

// _isValidAdaptor is used to check whether the adaptor name is correctly used or not
func (c *Config) _isValidAdaptor(name string) bool {

	// return true, if name exists in adaptor array
	for _, adaptor := range c.Adaptors {
		if adaptor.Name == name {
			return true
		}
	}

	// otherwise return false
	return false
}
