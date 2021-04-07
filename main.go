package main

import (
	"fmt"
	"github.com/klauspost/cpuid/v2"
	"github.com/tbhaxor/syncbit/utils"
	"os"
	"path"
	"sync"
)

// HandleTransfer is used to take backup, execute hooks and restore the zip file
func HandleTransfer(file utils.File, conn utils.SSHConnections, wg *sync.WaitGroup, conf utils.Config) {
	// when complete, mark it done
	defer wg.Done()

	// getting adopter details
	adaptors := []*utils.Adaptor{utils.GetAdaptorFromName(file.Src.Adaptor, conf), utils.GetAdaptorFromName(file.Dest.Adaptor, conf)}

	utils.Log.Infof("Backing up %s@%s:%s", adaptors[0].User, adaptors[0].Host, file.Src.Path)

	// ------ Source Transfer Begin ------
	utils.Log.Tracef("Executing global pre backup hooks")
	for _, step := range conf.Global.Hooks.PreBackup {
		if _, err := conn[file.Src.Adaptor].Run(step); err != nil {
			utils.Log.Tracef("Error while executing '%s' global pre backup hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed global pre backup hooks")

	utils.Log.Tracef("Executing scoped pre backup hooks")
	for _, step := range file.Src.PreBackup {
		if _, err := conn[file.Src.Adaptor].Run(fmt.Sprintf("cd %s && %s", file.Src.Path, step)); err != nil {
			utils.Log.Tracef("Error while executing '%s' global pre backup hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed scoped pre backup hooks")

	utils.Log.Tracef("Starting to zip %s@%s:%s", adaptors[0].User, adaptors[0].Host, file.Src.Path)
	// zip file in the directory
	if _, err := conn[file.Src.Adaptor].Run(fmt.Sprintf("cd %s && zip dump.zip -r .", file.Src.Path)); err != nil {
		utils.Log.Warnf("Skipping %s@%s:%s because zipping failed due to error: %s", adaptors[0].User, adaptors[0].Host, file.Src.Path, err.Error())
		return
	}
	utils.Log.Tracef("Completed zipping %s@%s:%s", adaptors[0].User, adaptors[0].Host, file.Src.Path)

	utils.Log.Tracef("Executing global post backup hooks")
	for _, step := range conf.Global.Hooks.PostBackup {
		if _, err := conn[file.Src.Adaptor].Run(step); err != nil {
			utils.Log.Tracef("Error while executing '%s' global post backup hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed global post backup hooks")

	utils.Log.Tracef("Executing scoped post backup hooks")
	for _, step := range file.Src.PostBackup {
		if _, err := conn[file.Src.Adaptor].Run(fmt.Sprintf("cd %s && %s", file.Src.Path, step)); err != nil {
			utils.Log.Tracef("Error while executing '%s' scoped post backup hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed scoped post backup hooks")

	utils.Log.Tracef("Executing global pre download hooks")
	for _, step := range conf.Global.Hooks.PreDownload {
		if _, err := conn[file.Src.Adaptor].Run(step); err != nil {
			utils.Log.Tracef("Error while executing '%s' global pre download hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed global pre download hooks")

	utils.Log.Tracef("Executing scoped pre download hooks")
	for _, step := range file.Src.PreDownload {
		if _, err := conn[file.Src.Adaptor].Run(fmt.Sprintf("cd %s && %s", file.Src.Path, step)); err != nil {
			utils.Log.Tracef("Error while executing '%s' scoped pre download hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed scoped pre download hooks")

	// download the file to local staging
	srcZip := fmt.Sprintf("%s/dump.zip", file.Src.Path)
	stagerName := utils.GetStagingFileName() + ".zip"
	destZip := path.Join(os.TempDir(), stagerName)

	// clean the files when the function is over
	defer os.Remove(destZip)
	defer utils.Log.Tracef("Removing %s", destZip)
	defer conn[file.Src.Adaptor].Run(fmt.Sprintf("rm -rf %s", srcZip))
	defer utils.Log.Tracef("Removing %s@%s:%s", adaptors[0].User, adaptors[0].Host, srcZip)
	defer conn[file.Src.Adaptor].Run(fmt.Sprintf("rm -rf /tmp/%s", stagerName))
	defer utils.Log.Tracef("Removing %s@%s:/tmp/%s", adaptors[1].User, adaptors[1].Host, stagerName)

	utils.Log.Tracef("Downloaded %s@%s:%s to %s in local", adaptors[0].User, adaptors[0].Host, srcZip, destZip)

	// finally download file to staging
	if err := conn[file.Src.Adaptor].Download(srcZip, destZip); err != nil {
		utils.Log.Warnf("Skipping %s@%s:%s because downloading zip failed due to error: %s", adaptors[0].User, adaptors[0].Host, file.Src.Path, err.Error())
		return
	}
	utils.Log.Tracef("Downloaded %s@%s:%s to %s in local", adaptors[0].User, adaptors[0].Host, srcZip, destZip)

	utils.Log.Tracef("Executing global post download hooks")
	for _, step := range conf.Global.Hooks.PostDownload {
		if _, err := conn[file.Src.Adaptor].Run(step); err != nil {
			utils.Log.Tracef("Error while executing '%s' global post download hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed global post download hooks")

	utils.Log.Tracef("Executing scoped post download hooks")
	for _, step := range file.Src.PostDownload {
		if _, err := conn[file.Src.Adaptor].Run(fmt.Sprintf("cd %s && %s", file.Src.PostDownload, step)); err != nil {
			utils.Log.Tracef("Error while executing '%s' scoped post download hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed scoped post download hooks")

	// ------ Destination Transfer Begins -------
	utils.Log.Infof("Restoring %s to %s@%s:%s", destZip, adaptors[1].User, adaptors[1].Host, file.Dest.Path)

	utils.Log.Tracef("Executing global pre upload hooks")
	for _, step := range conf.Global.Hooks.PreUpload {
		if _, err := conn[file.Dest.Adaptor].Run(step); err != nil {
			utils.Log.Tracef("Error while executing '%s' global pre upload hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed global pre upload hooks")

	utils.Log.Tracef("Executing scoped pre upload hooks")
	for _, step := range file.Dest.PreUpload {
		if _, err := conn[file.Dest.Adaptor].Run(fmt.Sprintf("cd %s && %s", file.Dest.Path, step)); err != nil {
			utils.Log.Tracef("Error while executing '%s' scoped pre upload hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed scoped pre upload hooks")

	utils.Log.Tracef("Uploading file from %s of local to %s@%s:/tmp/%s", destZip, adaptors[1].User, adaptors[1].Host, stagerName)
	// upload file to temporary directory
	if err := conn[file.Dest.Adaptor].Upload(destZip, fmt.Sprintf("/tmp/%s", stagerName)); err != nil {
		utils.Log.Warnf("Skipping %s@%s:%s because uploading zip failed due to error: %s", adaptors[1].User, adaptors[1].Host, file.Dest.Path, err.Error())
		return
	}
	utils.Log.Tracef("Uploaded file from %s of local to %s@%s:/tmp/%s", destZip, adaptors[1].User, adaptors[1].Host, stagerName)

	utils.Log.Tracef("Executing global post upload hooks")
	for _, step := range conf.Global.Hooks.PostUpload {
		if _, err := conn[file.Dest.Adaptor].Run(step); err != nil {
			utils.Log.Tracef("Error while executing '%s' global post upload hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed global post upload hooks")

	utils.Log.Tracef("Executing scoped post upload hooks")
	for _, step := range file.Dest.PostUpload {
		if _, err := conn[file.Dest.Adaptor].Run(fmt.Sprintf("cd %s && %s", file.Dest.Path, step)); err != nil {
			utils.Log.Tracef("Error while executing '%s' scoped post upload hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed scoped post upload hooks")

	utils.Log.Tracef("Executing global pre restore hooks")
	for _, step := range conf.Global.Hooks.PreRestore {
		if _, err := conn[file.Dest.Adaptor].Run(step); err != nil {
			utils.Log.Tracef("Error while executing '%s' global pre restore hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed global pre restore hooks")

	utils.Log.Tracef("Executing scoped pre restore hooks")
	for _, step := range file.Dest.PreRestore {
		if _, err := conn[file.Dest.Adaptor].Run(fmt.Sprintf("cd %s && %s", file.Dest.Path, step)); err != nil {
			utils.Log.Tracef("Error while executing '%s' scoped pre restore hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed scoped pre restore hooks")

	utils.Log.Tracef("Unzipping file from %s@%s:/tmp/%s to %s@%s:%s", adaptors[1].User, adaptors[1].Host, stagerName, adaptors[1].User, adaptors[1].Host, file.Dest.Path)
	// unzip the temp file to location in Dest.Path
	if _, err := conn[file.Dest.Adaptor].Run(fmt.Sprintf("unzip -o /tmp/%s -d %s", stagerName, file.Dest.Path)); err != nil {
		utils.Log.Warnf("Skipping %s@%s:%s because uploading zip failed due to error: %s", adaptors[1].User, adaptors[1].Host, file.Dest.Path, err.Error())
		return
	}
	utils.Log.Tracef("Done unzipping file from %s@%s:/tmp/%s to %s@%s:%s", adaptors[1].User, adaptors[1].Host, stagerName, adaptors[1].User, adaptors[1].Host, file.Dest.Path)

	utils.Log.Tracef("Executing global post restore hooks")
	for _, step := range conf.Global.Hooks.PostRestore {
		if _, err := conn[file.Dest.Adaptor].Run(step); err != nil {
			utils.Log.Tracef("Error while executing '%s' global post restore hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed global post restore hooks")

	utils.Log.Tracef("Executing scoped post restore hooks")
	for _, step := range file.Dest.PostRestore {
		if _, err := conn[file.Dest.Adaptor].Run(fmt.Sprintf("cd %s && %s", file.Dest.Path, step)); err != nil {
			utils.Log.Tracef("Error while executing '%s' scoped post restore hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Tracef("Completed scoped post restore hooks")

	utils.Log.Infof("%s@%s:%s has been successfully restored to %s@%s:%s", adaptors[0].User, adaptors[0].Host, file.Src.Path, adaptors[1].User, adaptors[1].Host, file.Dest.Path)
}

func main() {
	// get parsed config
	conf := utils.GetConfig()

	// get ssh connection
	conn := utils.GetSSHConnections(conf)

	// when this function call complete disconnect all ssh connections
	defer utils.DisconnectSSHConnections(conn)

	nThreads := cpuid.CPU.ThreadsPerCore * cpuid.CPU.PhysicalCores
	if nThreads > len(conf.Files) {
		utils.Log.Infof("Using %d workers", len(conf.Files))
	} else {
		utils.Log.Infof("Using %d workers", nThreads)
	}

	for _, chunk := range utils.ChunkifyFiles(conf.Files, nThreads) {
		var wg sync.WaitGroup

		for _, file := range chunk {
			wg.Add(1)
			go HandleTransfer(file, conn, &wg, conf)
		}

		wg.Wait()
	}

}
