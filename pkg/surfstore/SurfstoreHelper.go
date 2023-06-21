package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	// outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	outputMetaPath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))

	// fmt.Println("OUTPUT PATH MET A:", outputMetaPath)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	defer db.Close()

	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer statement.Close()
	statement.Exec()

	// panic("todo")
	statement, _ = db.Prepare(insertTuple)
	for fname, fmeta := range fileMetas {
		for idx, b := range fmeta.BlockHashList {

			_, err := statement.Exec(fname, fmeta.Version, idx, b)
			if err != nil {
				return fmt.Errorf("Error during Meta Write Back: %v", err)
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
// const getDistinctFileName string = `SELECT DISTINCT Filename
// 									FROM indexes
// 									ORDER BY Filename;`
const getDistinctFileName string = `SELECT DISTINCT fileName 
									FROM indexes;`

const getTuplesByFileName string = `SELECT *
									FROM indexes
									WHERE fileName = ?
									ORDER BY hashIndex;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	// fmt.Println("WE STARTINGGGG IT UP!")

	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	// metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, ""))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		fmt.Println("dedge")
		return fileMetaMap, nil
	}
	// fmt.Println("WE STARTINGGGG IT UP 2222!")
	db, err := sql.Open("sqlite3", metaFilePath)
	// fmt.Println("WE STARTINGGGG IT UP 33333!")
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	defer db.Close()
	// panic("todo")
	// retrieve all distinct FileNames

	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		fmt.Println(err.Error())
		return fileMetaMap, nil
	}

	// retrieve all tuple from FileNames and insert into map

	var fileName string
	var version int32
	var hashIndex int32
	var hashValue string
	for rows.Next() {
		rows.Scan(&fileName)
		tuple, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			fmt.Println(err.Error())
		}
		defer tuple.Close()
		for tuple.Next() {
			tuple.Scan(&fileName, &version, &hashIndex, &hashValue)
			// If the FileMetaData does not exist in the map, create a new one
			if _, ok := fileMetaMap[fileName]; !ok {
				fileMetaMap[fileName] = &FileMetaData{
					Filename: fileName,
					Version:  version,
					// Version:       1,
					BlockHashList: []string{hashValue},
				}
			} else {
				// If the FileMetaData already exists in the map, append to the existing BlockHashList
				fileMetaMap[fileName].BlockHashList = append(fileMetaMap[fileName].BlockHashList, hashValue)

			}
		}

		// // If the FileMetaData does not exist in the map, create a new one
		// fmt.Println("hash", hashIndex)
		// if _, ok := fileMetaMap[fileName]; !ok {
		// 	fileMetaMap[fileName] = &FileMetaData{
		// 		Filename: fileName,
		// 		Version:  version,
		// 		// Version:       1,
		// 		BlockHashList: []string{hashValue},
		// 	}
		// } else {
		// 	// If the FileMetaData already exists in the map, append to the existing BlockHashList
		// 	fileMetaMap[fileName].BlockHashList = append(fileMetaMap[fileName].BlockHashList, hashValue)

		// }
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
