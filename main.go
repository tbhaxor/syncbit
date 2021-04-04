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
	utils.Log.Debugf("Executing pre backup hooks")
	for _, step := range append(conf.Global.Hooks.PreBackup, file.Src.PreBackup...) {
		if _, err := conn[file.Src.Adaptor].Run(step); err != nil {
			utils.Log.Debugf("Error while executing '%s' pre backup hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Debugf("Completed pre backup hooks")

	// zip file in the directory
	if _, err := conn[file.Src.Adaptor].Run(fmt.Sprintf("cd %s && zip dump.zip -r .", file.Src.Path)); err != nil {
		utils.Log.Warnf("Skipping %s@%s:%s because zipping failed due to error: %s", adaptors[0].User, adaptors[0].Host, file.Src.Path, err.Error())
		return
	}

	utils.Log.Debugf("Executing post backup hooks")
	for _, step := range append(conf.Global.Hooks.PostBackup, file.Src.PostBackup...) {
		if _, err := conn[file.Src.Adaptor].Run(step); err != nil {
			utils.Log.Debugf("Error while executing '%s' post backup hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Debugf("Completed post backup hooks")

	utils.Log.Debugf("Executing pre download hooks")
	for _, step := range append(conf.Global.Hooks.PreDownload, file.Src.PreDownload... ) {
		if _, err := conn[file.Src.Adaptor].Run(step); err != nil {
			utils.Log.Debugf("Error while executing '%s' pre download hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Debugf("Completed pre download hooks")

	// download the file to local staging
	srcZip := fmt.Sprintf("%s/dump.zip", file.Src.Path)
	stagerName := utils.GetStagingFileName() + ".zip"
	destZip := path.Join(os.TempDir(), stagerName)

	// clean the files when the function is over
	defer os.Remove(destZip)
	defer utils.Log.Debugf("Removing %s", destZip)
	defer conn[file.Src.Adaptor].Run(fmt.Sprintf("rm -rf %s", srcZip))
	defer utils.Log.Debugf("Removing %s@%s:%s", adaptors[0].User, adaptors[0].Host, srcZip)
	defer conn[file.Src.Adaptor].Run(fmt.Sprintf("rm -rf /tmp/%s", stagerName))
	defer utils.Log.Debugf("Removing %s@%s:/tmp/%s", adaptors[1].User, adaptors[1].Host, stagerName)

	utils.Log.Debugf("Downloading %s@%s:%s to %s in local", adaptors[0].User, adaptors[0].Host, srcZip, destZip)

	// finally download file to staging
	if err := conn[file.Src.Adaptor].Download(srcZip, destZip); err != nil {
		utils.Log.Warnf("Skipping %s@%s:%s because downloading zip failed due to error: %s", adaptors[0].User, adaptors[0].Host, file.Src.Path, err.Error())
		return
	}

	utils.Log.Debugf("Executing post download hooks")
	for _, step := range append(conf.Global.Hooks.PostDownload, file.Src.PostDownload...) {
		if _, err := conn[file.Src.Adaptor].Run(step); err != nil {
			utils.Log.Debugf("Error while executing '%s' post download hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Debugf("Completed post download hooks")

	// ------ Destination Transfer Begins -------
	utils.Log.Infof("Restoring %s to %s@%s:%s", destZip, adaptors[1].User, adaptors[1].Host, file.Dest.Path)

	utils.Log.Debugf("Executing pre upload hooks")
	for _, step := range append(conf.Global.Hooks.PreUpload, file.Dest.PreUpload...) {
		if _, err := conn[file.Dest.Adaptor].Run(step); err != nil {
			utils.Log.Debugf("Error while executing '%s' pre upload hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Debugf("Completed pre upload hooks")

	// upload file to temporary directory
	if err := conn[file.Dest.Adaptor].Upload(destZip , fmt.Sprintf("/tmp/%s", stagerName)); err != nil {
		utils.Log.Warnf("Skipping %s@%s:%s because uploading zip failed due to error: %s", adaptors[1].User, adaptors[1].Host, file.Dest.Path, err.Error())
		return
	}

	utils.Log.Debugf("Executing post upload hooks")
	for _, step := range append(conf.Global.Hooks.PostUpload, file.Dest.PostUpload...) {
		if _, err := conn[file.Dest.Adaptor].Run(step); err != nil {
			utils.Log.Debugf("Error while executing '%s' post upload hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Debugf("Completed post upload hooks")

	utils.Log.Debugf("Executing pre restore hooks")
	for _, step := range append(conf.Global.Hooks.PreRestore, file.Dest.PreUpload...) {
		if _, err := conn[file.Dest.Adaptor].Run(step); err != nil {
			utils.Log.Debugf("Error while executing '%s' pre restore hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Debugf("Completed pre restore hooks")

	// unzip the temp file to location in Dest.Path
	if _, err := conn[file.Dest.Adaptor].Run(fmt.Sprintf("unzip -o /tmp/%s -d %s", stagerName, file.Dest.Path)); err != nil {
		utils.Log.Warnf("Skipping %s@%s:%s because uploading zip failed due to error: %s", adaptors[1].User, adaptors[1].Host, file.Dest.Path, err.Error())
		return
	}

	utils.Log.Debugf("Executing post restore hooks")
	for _, step := range append(conf.Global.Hooks.PostRestore, file.Dest.PostRestore...) {
		if _, err := conn[file.Dest.Adaptor].Run(step); err != nil {
			utils.Log.Debugf("Error while executing '%s' post restore hook. Error message: %s", step, err.Error())
		}
	}
	utils.Log.Debugf("Completed pre restore hooks")
	utils.Log.Infof("%s@%s:%s has been successfully restored to %s@%s:%s", adaptors[0].User, adaptors[0].Host, file.Src.Path, adaptors[1].User, adaptors[1].Host, file.Dest.Path)
	utils.Log.Debugf("Removing %s@%s:/tmp%s", adaptors[1].User, adaptors[1].Host, stagerName)
}

func main() {
	// get parsed config
	conf := utils.GetConfig()

	// get ssh connection
	conn := utils.GetSSHConnections(conf)

	// when this function call complete disconnect all ssh connections
	defer utils.DisconnectSSHConnections(conn)

	nThreads := cpuid.CPU.ThreadsPerCore * cpuid.CPU.PhysicalCores
	utils.Log.Infof("Using %d workers", nThreads)

	for _, chunk := range utils.ChunkifyFiles(conf.Files, nThreads) {
		var wg sync.WaitGroup

		for _, file := range chunk {
			wg.Add(1)
			go HandleTransfer(file, conn, &wg, conf)
		}

		wg.Wait()
	}

}
