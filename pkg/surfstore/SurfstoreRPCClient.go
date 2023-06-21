package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	(*block).BlockData = b.BlockData

	(*block).BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// panic("todo")
	// fmt.Println("Block store address in PutBlock is:", blockStoreAddr)
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// panic("todo")
	// fmt.Println("Block store address in HasBlocks is:", blockStoreAddr)
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashes := &BlockHashes{}
	blockHashes.Hashes = blockHashesIn
	blockHashesRet, err := c.HasBlocks(ctx, blockHashes)
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = blockHashesRet.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// panic("todo")

	// conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}

		*serverFileInfoMap = fileInfoMap.FileInfoMap

		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("get fileinfomap operation: failed to contact all servers")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		finalVers, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			continue
		}
		*latestVersion = finalVers.Version

		// close the connection
		return conn.Close()
	}
	return fmt.Errorf("update file operation: failed to contact all servers")
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bh, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	*blockHashes = bh.GetHashes()
	return err
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			// fmt.Println("Error inside dial:", err)
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		blockHashes := &BlockHashes{}
		for _, bin := range blockHashesIn {
			blockHashes.Hashes = append(blockHashes.Hashes, bin)
		}
		bm, err := c.GetBlockStoreMap(ctx, blockHashes)
		if err != nil {
			continue
		}
		bStrMp := make(map[string][]string)
		for k, v := range bm.BlockStoreMap {
			bStrMp[k] = v.GetHashes()
		}
		*blockStoreMap = bStrMp
		return err
	}
	return fmt.Errorf("get block str map operation: failed to contact all servers")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		addrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			continue
		}
		*blockStoreAddrs = addrs.GetBlockStoreAddrs()

		return conn.Close()
	}
	return fmt.Errorf("get block addrs operation: failed to contact all servers")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
