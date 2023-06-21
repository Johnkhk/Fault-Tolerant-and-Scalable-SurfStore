package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string // map fom server hash to server
	Hashes    []string          // sorted hashes
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	responsibleServer := ""
	for i := 0; i < len(c.Hashes); i++ {
		if c.Hashes[i] > blockId {
			responsibleServer = c.ServerMap[c.Hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[c.Hashes[0]]
	}
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")
	var CHR ConsistentHashRing
	hashes := []string{}
	serverMap := make(map[string]string) // serverhash: serverName
	for _, serverName := range serverAddrs {
		// serverName = "blockstore" + serverName
		serverHash := CHR.Hash("blockstore" + serverName)
		serverMap[serverHash] = serverName
		hashes = append(hashes, serverHash)
	}
	sort.Strings(hashes)
	CHR.Hashes = hashes
	CHR.ServerMap = serverMap
	return &CHR
}
