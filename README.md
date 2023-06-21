# Fault Tolerant & Scalable Surf Store #

## What is Fault Tolerant & Scalable Surf Store? ##
Extending on the original [surfstore](https://github.com/Johnkhk/Surf-Store), this project implements scalability, which adds support for multiple BlockStores to handle the contents of a large service. This project also implements fault tolerancy by implementing the [Raft Consensus Algorithm](https://raft.github.io/), log replicating MetaStores and allowing effectively allows multiple nodes (servers) to work together in a cluster to achieve consensus on a shared state.. 

Surf store is a cloud based file storage and syncing system like Drop Box.<br>
It is based on protocol buffers using (gRPC Remote Procedure Calls). <br>
There is a MetaStore for and a BlockStore service.

### Scalability with multiple BlockStores ###
Consistent hashing is a technique used in SurfStore's deployment with 1000 block store servers to determine the appropriate server for storing each block. Instead of relying on a single index server or random selection, consistent hashing creates a hash ring where each block store server is assigned a name based on its address using a hash function. When updating a file, the GetBlockStoreMap function is called, leveraging consistent hashing to obtain a map indicating which block servers should store the respective blocks based on their hash values. This approach ensures efficient load balancing, scalability, and eliminates bottlenecks by distributing blocks across servers and minimizing data redistribution when server sets change, ultimately enhancing the efficiency of the distributed storage system.

### Raft Consensus Algorithm ###
The Raft consensus algorithm is implemented in SurfStore to ensure fault tolerance, availability, and consistency of the metadata service. By replicating the MetaStore as a replicated state machine using the Raft protocol, concurrent updates to file metadata by multiple clients can be handled reliably. The log replication part of the Raft protocol is implemented in this project to achieve fault tolerance. The RaftSurfstoreServer, functioning as the fault-tolerant MetaStore, communicates with other servers via gRPC. Each server is aware of all other servers and does not dynamically join the cluster. Leader assignment is done through the SetLeader API call, eliminating the need for elections. Clients can interact with the system successfully as long as a majority quorum of nodes can be queried by the leader. If a majority of nodes are crashed, clients will block and wait until a majority are restored. Clients interacting with non-leader nodes will receive an error message and should retry to find the leader. The RaftTestingInterface and ChaosMonkey are used for testing, simulating server crashes and failures to ensure the correctness and adherence to Raft's properties.
<p align="center">
<img src="misc/surfsto.png" alt="isolated" width="600"/>
</p>

## How to use Surf Store? ##
1. Download and Install Go https://go.dev/doc/install
2. Download plugins for gRPC
   1. go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
   2. go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
3. Then run the server
   1. go run cmd/SurfstoreServerExec/main.go -s &lt;service> -p &lt;port> -l -d (BlockStoreAddr*)
      1. &lt;service> can be meta store, block store, or both. (run both services on the same port)
      2. e.g: go run cmd/SurfstoreServerExec/main.go -s both -p 8081 -l localhost:8081
      3. You can also use **make run-both**
4. Then run client (Sync)
   1. Once you create a folder (e.g dataA), provide a path to that folder to sync
   2. Before you sync to the server, you can modify or delete files. For multiple clients, syncing will be based on versioning.
   3. go run cmd/SurfstoreClientExec/main.go server_addr:port &lt;folder path> &lt;blockSize>
      1. blockSize is how big are the blocks in bytes for the data.
      2. e.g go run cmd/SurfstoreClientExec/main.go localhost:8081 dataA 4096
      3. e.g go run cmd/SurfstoreClientExec/main.go localhost:8081 dataB 4096# Fault-Tolerant-and-Scalable-SurfStore
