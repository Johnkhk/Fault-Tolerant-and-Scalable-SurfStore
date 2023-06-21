package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
	// mutex
	mtx sync.Mutex
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	// check whether the file is in the FileMetaMap
	fName := fileMetaData.Filename
	fVersion := fileMetaData.Version
	if _, ok := m.FileMetaMap[fName]; ok {
		if fVersion == m.FileMetaMap[fName].Version+1 {
			// if fVersion+1 == m.FileMetaMap[fName].Version {
			m.FileMetaMap[fName] = fileMetaData
		} else {
			// fmt.Println(fVersion, m.FileMetaMap[fName].Version)
			// failed to update
			fVersion = -1
		}
	} else {
		// not in the map, just add it
		m.FileMetaMap[fName] = fileMetaData
	}
	return &Version{Version: fVersion}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	// fmt.Println("INSIDE GetBlockStoreMap: ")
	BSM := make(map[string]*BlockHashes)
	for _, hash := range blockHashesIn.Hashes {
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		// fmt.Println("hash:", hash, " server:", server)
		_, found := BSM[server]
		if !found {
			BSM[server] = &BlockHashes{}
		}
		BSM[server].Hashes = append(BSM[server].Hashes, hash)
	}

	return &BlockStoreMap{BlockStoreMap: BSM}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
