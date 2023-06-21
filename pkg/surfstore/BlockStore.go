package surfstore

import (
	context "context"
	"fmt"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	mtx sync.Mutex
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	if val, found := bs.BlockMap[blockHash.GetHash()]; found {
		return val, nil
	}
	return nil, fmt.Errorf("Block %v is not found in the map", blockHash.GetHash())
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	blockHash := GetBlockHashString(block.BlockData)
	bs.BlockMap[blockHash] = block
	return &Success{
		Flag: true,
	}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// panic("todo")
	bs.mtx.Lock()
	defer bs.mtx.Unlock()
	var h []string
	for i := 0; i < len(blockHashesIn.Hashes); i++ {
		_, found := bs.BlockMap[blockHashesIn.Hashes[i]]
		if found {
			h = append(h, blockHashesIn.Hashes[i])
		}
	}
	return &BlockHashes{
		Hashes: h,
	}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")
	hashes := []string{}
	for hash := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
