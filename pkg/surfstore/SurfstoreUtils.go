package surfstore

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	fmt.Println("SYNCING", client.BaseDir)
	// 1. Scan base directory
	indexPath := client.BaseDir + "/index.db"
	if _, err := os.Stat(indexPath); errors.Is(err, os.ErrNotExist) {
		os.Create(indexPath)
	}
	// var blockAddrs []string
	// err := client.GetBlockStoreAddrs(&blockAddrs)
	// check(err)
	dir, err := ioutil.ReadDir(client.BaseDir)
	check(err)
	// 2. Compute each file's hash list
	curIndex := make(map[string][]string) // map of local files to hashList
	for _, dirFile := range dir {
		if dirFile.Name() != "index.db" {
			curIndex[dirFile.Name()] = getHashList(client, dirFile.Name())
		}
	}
	// get localIndex
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir) // retrieved from index.db
	check(err)
	fmt.Println("Read Local Index")
	PrintMetaMap(localIndex)

	// 4. Client connect to server and download updated FileInfoMap
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	check(err)
	fmt.Println("REMOTE: ")
	PrintMetaMap(remoteIndex)

	newlocal := make(map[string]*FileMetaData)

	// case 1
	for serverfname, serverfmeta := range remoteIndex {
		_, inLocalIndex := localIndex[serverfname]
		_, inCurIndex := curIndex[serverfname]
		fmt.Println(serverfname)
		fmt.Println(inLocalIndex, inCurIndex)

		// not in local index
		// if !inLocalIndex {
		// 	localIndex[serverfname] = remoteIndex[serverfname]
		// }
		// if !inCurIndex {
		// 	fmt.Println("downloading")
		// 	downloadFile(client, blockAddr, serverfname, serverfmeta.BlockHashList)
		// }
		// if !inLocalIndex || !inCurIndex {
		if !inLocalIndex {

			downloadFile(client, "", serverfname, serverfmeta.BlockHashList)
			// localIndex[serverfname] = remoteIndex[serverfname]
			newlocal[serverfname] = remoteIndex[serverfname]
			// defer func(serverfname string) { localIndex[serverfname] = remoteIndex[serverfname] }(serverfname)

		}

		if !inCurIndex && inLocalIndex {
			if localIndex[serverfname].Version == serverfmeta.Version && len(localIndex[serverfname].BlockHashList) == 1 && localIndex[serverfname].BlockHashList[0] == "0" {
				fmt.Println("tombstoned, no need to update Version")
				continue
			}
			fmt.Println("File deleted locally, updatin server")
			var newHashList []string
			newHashList = append(newHashList, "0")
			updateFileMetaData := &FileMetaData{
				Filename:      serverfname,
				Version:       localIndex[serverfname].Version + 1,
				BlockHashList: newHashList,
			}
			latestVersion := new(int32)
			err = client.UpdateFile(updateFileMetaData, latestVersion)
			check(err)
			var zero int32 = 0
			// delete unsuccessful
			if *latestVersion < zero {
				fmt.Println("delete unsucc")
				err = client.GetFileInfoMap(&remoteIndex)
				check(err)
				downloadFile(client, "", serverfname, remoteIndex[serverfname].BlockHashList)
				localIndex[serverfname] = remoteIndex[serverfname]
				// localIndex[serverfname] = remoteIndex[serverfname]

			}
		}

		// if inLocalIndex && inCurIndex {
		// 	fmt.Println("BROO")

		// 	if !Equal(localIndex[serverfname].BlockHashList, curIndex[serverfname]) {
		// 		fmt.Println("MODIFIED")

		// 		dirBlockArr := getBlockArray(client, serverfname)
		// 		// upload blocks to server
		// 		for _, newBlocks := range dirBlockArr {
		// 			var succ bool
		// 			err = client.PutBlock(newBlocks, blockAddr, &succ)
		// 			check(err)
		// 		}
		// 		// update server with new Fileinfo
		// 		Fileinfo := &FileMetaData{
		// 			Filename:      serverfname,
		// 			Version:       serverfmeta.Version + 1,
		// 			BlockHashList: serverfmeta.BlockHashList,
		// 		}
		// 		latestVersion := new(int32)
		// 		err = client.UpdateFile(Fileinfo, latestVersion)
		// 		check(err)
		// 		var zero int32 = 0
		// 		if *latestVersion >= zero { // upload success
		// 			// defer func(serverfname string) { localIndex[serverfname] = Fileinfo }(serverfname)
		// 			newlocal[serverfname] = remoteIndex[serverfname]
		// 		} else {
		// 			fmt.Println("FAILED")
		// 		}
		// 	}
		// }

		// if !inCurIndex {
		// 	fmt.Println("Here -2")
		// 	fmt.Println("downloading remote to local")
		// 	downloadFile(client, blockAddr, serverfname, serverfmeta.BlockHashList)
		// 	fmt.Println("downloading Done")
		// 	localIndex[serverfname] = remoteIndex[serverfname]
		// }
	}

	// case 2
	for curFileName, curHashList := range curIndex {
		_, inLocalIndex := localIndex[curFileName]
		_, inRemoteIndex := remoteIndex[curFileName]
		if !inLocalIndex || !inRemoteIndex {

			fmt.Println("Here -1")
			// upload blocks to server
			// dirBlockArr := getBlockArray(client, curFileName)
			// for _, newBlocks := range dirBlockArr {
			// 	var succ bool
			// 	err = client.PutBlock(newBlocks, blockAddr, &succ)
			// 	// up
			// 	check(err)
			// }
			// err = ServerFileUpload(metaData, baseDir+"/"+filename, client)

			var blockStoreMap map[string][]string // server: hashes[]
			hashes := []string{}
			hashAndBlock := make(map[string]Block)
			dirBlockArr := getBlockArray(client, curFileName)
			for _, b := range dirBlockArr {
				bhash := GetBlockHashString(b.BlockData)
				fmt.Println("BHASH: ", bhash)
				hashes = append(hashes, bhash)
				hashAndBlock[bhash] = *b
			}
			client.GetBlockStoreMap(hashes, &blockStoreMap)
			for server, hashes := range blockStoreMap {
				for _, hash := range hashes {
					metaDataBlock := hashAndBlock[hash]
					var succ bool
					err := client.PutBlock(&metaDataBlock, server, &succ)
					if err != nil {
						log.Println("Failed to put block: ", err)
					}
				}
			}

			// update server with new Fileinfo
			Fileinfo := &FileMetaData{
				Filename:      curFileName,
				Version:       1,
				BlockHashList: curHashList,
			}
			for _, s := range curHashList {
				fmt.Println("WE IN HIA", s)
			}
			latestVersion := new(int32)
			err = client.UpdateFile(Fileinfo, latestVersion)
			check(err)
			var zero int32 = 0
			// if *latestVersion >= zero { // upload success
			if *latestVersion > zero { // upload success
				newlocal[curFileName] = Fileinfo
			} else {
				fmt.Println("Here 0", *latestVersion)
				if inRemoteIndex {
					downloadFile(client, "", curFileName, remoteIndex[curFileName].BlockHashList)
					// defer func(curFileName string) { localIndex[curFileName] = remoteIndex[curFileName] }(curFileName)
					newlocal[curFileName] = remoteIndex[curFileName]

				}

				// version mishap
				// curHashList,

				// case 1 no local modifications
				// if Equal(curHashList, localIndex[curFileName].BlockHashList)

				// case 2 local changes, remote and local same version. then sync to remote. if success sync local

				// case 3 local changes, remote ver > local index ver. download blocks, bring local version up to date

			}
		} else if inLocalIndex && inRemoteIndex { // all have the file
			localChanged := !Equal(curHashList, localIndex[curFileName].BlockHashList)
			if localChanged {
				fmt.Println("MODMODMOD")
			}
			if !localChanged && remoteIndex[curFileName].Version > localIndex[curFileName].Version {
				fmt.Println("Here 1")
				downloadFile(client, "", curFileName, remoteIndex[curFileName].BlockHashList)
				localIndex[curFileName] = remoteIndex[curFileName]
			} else if localChanged && remoteIndex[curFileName].Version == localIndex[curFileName].Version {
				fmt.Println("Here 2")
				// upload blocks to server
				// dirBlockArr := getBlockArray(client, curFileName)
				// for _, newBlocks := range dirBlockArr {
				// 	var succ bool
				// 	err = client.PutBlock(newBlocks, "", &succ)
				// 	check(err)
				// }
				var blockStoreMap map[string][]string
				dirBlockArr := getBlockArray(client, curFileName)
				hashes := []string{}
				hashAndBlock := make(map[string]Block)
				for _, b := range dirBlockArr {
					hashes = append(hashes, GetBlockHashString(b.BlockData))
					hashAndBlock[GetBlockHashString(b.BlockData)] = *b
				}
				client.GetBlockStoreMap(hashes, &blockStoreMap)
				for server, hashes := range blockStoreMap {
					for _, hash := range hashes {
						metaDataBlock := hashAndBlock[hash]
						var succ bool
						err := client.PutBlock(&metaDataBlock, server, &succ)
						if err != nil {
							log.Println("Failed to put block: ", err)
						}
					}
				}
				// update server with new Fileinfo
				Fileinfo := &FileMetaData{
					Filename:      curFileName,
					Version:       remoteIndex[curFileName].Version + 1,
					BlockHashList: curHashList,
				}
				latestVersion := new(int32)
				err = client.UpdateFile(Fileinfo, latestVersion)
				check(err)
				var zero int32 = 0
				if *latestVersion >= zero { // upload success
					// localIndex[curFileName] = Fileinfo
					fmt.Println("upload success")
					newlocal[curFileName] = Fileinfo
				} else {
					fmt.Println("upload failed")
				}
			} else if localChanged && remoteIndex[curFileName].Version > localIndex[curFileName].Version {
				err = client.GetFileInfoMap(&remoteIndex)
				check(err)
				downloadFile(client, "", curFileName, remoteIndex[curFileName].BlockHashList)
				newlocal[curFileName] = remoteIndex[curFileName]
			}
		}
	}

	err = client.GetFileInfoMap(&localIndex)
	check(err)
	for a, b := range newlocal {
		localIndex[a] = b
	}
	fmt.Println("writing the following to index.db")
	PrintMetaMap(localIndex)

	fmt.Println(client.BaseDir)
	err = WriteMetaFile(localIndex, client.BaseDir)
	check(err)
	fmt.Println("After write check")
	localIndexz, err := LoadMetaFromMetaFile(client.BaseDir) // retrieved from index.db
	PrintMetaMap(localIndexz)

	// err = client.GetFileInfoMap(&localIndex)
	// check(err)
	// PrintMetaMap(localIndex)

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func getHashList(client RPCClient, fileName string) []string {
	var bHashList []string
	filePath := ConcatPath(client.BaseDir, fileName)
	f, err := os.Open(filePath)
	check(err)
	for {
		buffer := make([]byte, client.BlockSize)
		n, err := f.Read(buffer) //readFull?
		if err != nil {
			if err == io.EOF {
				bHashList = append(bHashList, GetBlockHashString(buffer[:n]))
				break
			} else {
				check(err)
			}
		} else {
			bHashList = append(bHashList, GetBlockHashString(buffer[:n]))
		}
	}
	// return bHashList
	return bHashList[:len(bHashList)-1]

}

func downloadFile(client RPCClient, blockAdd string, fileName string, fileHash []string) {
	//fmt.Println(fileName)
	var blockStoreAddrs []string
	err := client.GetBlockStoreAddrs(&blockStoreAddrs)
	for _, s := range blockStoreAddrs {
		fmt.Println(s)
	}
	check(err)

	filePath := ConcatPath(client.BaseDir, fileName)
	_, err = os.Create(filePath)
	check(err)
	err = os.Remove(filePath)
	check(err)
	file, err := os.Create(filePath)
	// defer file.Close()

	// blockHashesIn := serverMetaData.BlockHashList
	// *clientMetaData = *serverMetaData

	var blockStoreMap map[string][]string
	// log.Println("Fetching the servers for each block hash")
	client.GetBlockStoreMap(fileHash, &blockStoreMap)
	// log.Println("Fetched the servers for each block hash->", blockStoreMap)

	check(err)
	if len(fileHash) == 1 && fileHash[0] == "0" {
		err := os.Remove(filePath)
		check(err)
	}

	fmt.Println("Downloading file: ", fileHash)
	// writeToFileData := ""

	metaDataAndBlockMap := make(map[string]Block)
	// metaDataAndBlockMap := make(map[string][]byte)

	for server, hashes := range blockStoreMap {
		fmt.Println("server: ", server)
		for _, hash := range hashes {
			var metaDataBlock Block
			if err := client.GetBlock(hash, server, &metaDataBlock); err != nil {
				log.Println("Error while getting block: ", err)
				return
			}
			// file.Write(metaDataBlock.BlockData)
			metaDataAndBlockMap[hash] = metaDataBlock
		}
	}
	for _, metaDataHash := range fileHash {
		// fmt.Println("filehasj: ", fileHash)
		// writeToFileData += string(metaDataAndBlockMap[metaDataHash].BlockData)
		// blockDat =
		// fmt.Println("WHAT: ", metaDataHash)
		file.Write(metaDataAndBlockMap[metaDataHash].BlockData)
	}
	// file.WriteString(writeToFileData)

	// else {
	// 	fileByte := &Block{}
	// 	for _, h := range fileHash {
	// 		err := client.GetBlock(h, blockAdd, fileByte)
	// 		check(err)
	// 		_, err = file.Write(fileByte.BlockData)
	// 		check(err)
	// 	}
	// }
}

// gets block arrays from basedir file
func getBlockArray(client RPCClient, fileName string) []*Block {
	var blockArr []*Block
	filePath := ConcatPath(client.BaseDir, fileName)
	f, err := os.Open(filePath)
	check(err)
	for {
		buffer := make([]byte, client.BlockSize)
		var curBlock Block
		n, err := f.Read(buffer)
		if err != nil {
			if err == io.EOF {
				curBlock.BlockData = buffer[:n]
				curBlock.BlockSize = int32(n)
				blockArr = append(blockArr, &curBlock)
				break
			} else {
				check(err)
			}
		} else {
			curBlock.BlockData = buffer[:n]
			curBlock.BlockSize = int32(n)
			blockArr = append(blockArr, &curBlock)
		}
	}
	// return blockArr
	// fmt.Println("LENGTH IF BLOCK ARRAY before", len(blockArr))
	ret := blockArr[:len(blockArr)-1]
	// fmt.Println("LENGTH IF BLOCK ARRAY after", len(ret))

	return ret

}

// equal function compare two array
func Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

/*

map[2013e3d7aab7279107a310e8707a2f2630b3c04882fc51b2035ce93520b215cd:localhost8081 2b1c07fb559dd83e4beeead52987b0ee87670fa6ed2bd36d534238e34ebd9e8e:localhost8081 3984d4cd42eb16150faec87fa27ecccbe8541b86d57fde4f86c32baf78659563:localhost8081 3b6bc054235d0a91b72fab166cf9fe3b3b05a08d8455e73ceb0685a8b4b8519a:localhost8081 4086e8322c8dffbd5914d5f027e02fdd14b258245a2882bac428ba296e39844c:localhost8081 43fd58b4f74b8221a180cd30687794ef6a802bbace78df7cb75dad7006eda31b:localhost8081 4fd42677571a191f629875cca7f8fc8905c49a16c13bc049268f9a8eb8ad20ab:localhost8081 56824244dd4db46d901deed1061ffd9f4e67185ec2c9f51eae258d1a3aba51e3:localhost8081 7b7c7c3cf9fec3f801db08ad37443f77d20be8ad0d1d61acf7bc82111fe89ee0:localhost8081 7c8a32ea1c550f4d6eb988e461af1151d0ab48d0c308e83e1a2a00673cbe52be:localhost8081 82bae01e9a454bf73b920978a17236ca1f49ae0234c5e878c6345d8dd7c6270f:localhost8081 8e8d81badeab2357becbf0feec7f18b2198b51f63f855c1deb8f16310d64d2bc:localhost8081 c0d6e59a60b0f1b803f695c9575e2ea6f497d7fa06c2a13dd1fa1c49b9d33412:localhost8081 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855:localhost8081 fa57b1b78ba30f9e30cb487824df16ee663914ad13d3c51d8dc0f660d49643c0:localhost8081]
map[2013e3d7aab7279107a310e8707a2f2630b3c04882fc51b2035ce93520b215cd:localhost8081 2b1c07fb559dd83e4beeead52987b0ee87670fa6ed2bd36d534238e34ebd9e8e:localhost8081 3984d4cd42eb16150faec87fa27ecccbe8541b86d57fde4f86c32baf78659563:localhost8081 3b6bc054235d0a91b72fab166cf9fe3b3b05a08d8455e73ceb0685a8b4b8519a:localhost8081 4086e8322c8dffbd5914d5f027e02fdd14b258245a2882bac428ba296e39844c:localhost8081 43fd58b4f74b8221a180cd30687794ef6a802bbace78df7cb75dad7006eda31b:localhost8081 4fd42677571a191f629875cca7f8fc8905c49a16c13bc049268f9a8eb8ad20ab:localhost8081 56824244dd4db46d901deed1061ffd9f4e67185ec2c9f51eae258d1a3aba51e3:localhost8081 7b7c7c3cf9fec3f801db08ad37443f77d20be8ad0d1d61acf7bc82111fe89ee0:localhost8081 7c8a32ea1c550f4d6eb988e461af1151d0ab48d0c308e83e1a2a00673cbe52be:localhost8081 82bae01e9a454bf73b920978a17236ca1f49ae0234c5e878c6345d8dd7c6270f:localhost8081 8e8d81badeab2357becbf0feec7f18b2198b51f63f855c1deb8f16310d64d2bc:localhost8081 c0d6e59a60b0f1b803f695c9575e2ea6f497d7fa06c2a13dd1fa1c49b9d33412:localhost8081 fa57b1b78ba30f9e30cb487824df16ee663914ad13d3c51d8dc0f660d49643c0:localhost8081]
*/

/*
map[2013e3d7aab7279107a310e8707a2f2630b3c04882fc51b2035ce93520b215cd:localhost8081 2b1c07fb559dd83e4beeead52987b0ee87670fa6ed2bd36d534238e34ebd9e8e:localhost8081 3984d4cd42eb16150faec87fa27ecccbe8541b86d57fde4f86c32baf78659563:localhost8081 3b6bc054235d0a91b72fab166cf9fe3b3b05a08d8455e73ceb0685a8b4b8519a:localhost8081 4086e8322c8dffbd5914d5f027e02fdd14b258245a2882bac428ba296e39844c:localhost8081 43fd58b4f74b8221a180cd30687794ef6a802bbace78df7cb75dad7006eda31b:localhost8081 4fd42677571a191f629875cca7f8fc8905c49a16c13bc049268f9a8eb8ad20ab:localhost8081 56824244dd4db46d901deed1061ffd9f4e67185ec2c9f51eae258d1a3aba51e3:localhost8081 7b7c7c3cf9fec3f801db08ad37443f77d20be8ad0d1d61acf7bc82111fe89ee0:localhost8081 7c8a32ea1c550f4d6eb988e461af1151d0ab48d0c308e83e1a2a00673cbe52be:localhost8081 82bae01e9a454bf73b920978a17236ca1f49ae0234c5e878c6345d8dd7c6270f:localhost8081 8e8d81badeab2357becbf0feec7f18b2198b51f63f855c1deb8f16310d64d2bc:localhost8081 c0d6e59a60b0f1b803f695c9575e2ea6f497d7fa06c2a13dd1fa1c49b9d33412:localhost8081 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855:localhost8081 fa57b1b78ba30f9e30cb487824df16ee663914ad13d3c51d8dc0f660d49643c0:localhost8081]
map[2013e3d7aab7279107a310e8707a2f2630b3c04882fc51b2035ce93520b215cd:localhost8081 2b1c07fb559dd83e4beeead52987b0ee87670fa6ed2bd36d534238e34ebd9e8e:localhost8081 3984d4cd42eb16150faec87fa27ecccbe8541b86d57fde4f86c32baf78659563:localhost8081 3b6bc054235d0a91b72fab166cf9fe3b3b05a08d8455e73ceb0685a8b4b8519a:localhost8081 4086e8322c8dffbd5914d5f027e02fdd14b258245a2882bac428ba296e39844c:localhost8081 43fd58b4f74b8221a180cd30687794ef6a802bbace78df7cb75dad7006eda31b:localhost8081 4fd42677571a191f629875cca7f8fc8905c49a16c13bc049268f9a8eb8ad20ab:localhost8081 56824244dd4db46d901deed1061ffd9f4e67185ec2c9f51eae258d1a3aba51e3:localhost8081 7b7c7c3cf9fec3f801db08ad37443f77d20be8ad0d1d61acf7bc82111fe89ee0:localhost8081 7c8a32ea1c550f4d6eb988e461af1151d0ab48d0c308e83e1a2a00673cbe52be:localhost8081 82bae01e9a454bf73b920978a17236ca1f49ae0234c5e878c6345d8dd7c6270f:localhost8081 8e8d81badeab2357becbf0feec7f18b2198b51f63f855c1deb8f16310d64d2bc:localhost8081 c0d6e59a60b0f1b803f695c9575e2ea6f497d7fa06c2a13dd1fa1c49b9d33412:localhost8081 fa57b1b78ba30f9e30cb487824df16ee663914ad13d3c51d8dc0f660d49643c0:localhost8081]
*/
