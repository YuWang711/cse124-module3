package surfstore

import (
	"net/rpc"
	"log"
)

type RPCClient struct {
	ServerAddr string
	BaseDir    string
	BlockSize  int
}

func (surfClient *RPCClient) GetBlock(blockHash string, block *Block) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", "localhost:"+surfClient.ServerAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Surfstore.GetBlock", blockHash, block)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block Block, succ *bool) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", "localhost:"+surfClient.ServerAddr)
	if e != nil {
		return e
	}
	e = conn.Call("Surfstore.PutBlock", block, succ)
	log.Print("PUTBLOCK - RPCClient success:", *succ)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	// connect to the server
	conn, e := rpc.DialHTTP("tcp", "localhost:"+surfClient.ServerAddr)
	if e != nil {
		return e
	}

	// perform the call
	e = conn.Call("Surfstore.HasBlcoks", blockHashesIn, blockHashesOut)
	if e != nil {
		conn.Close()
		return e
	}

	// close the connection
	return conn.Close()
}
// Asking client for remote index
func (surfClient *RPCClient) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
	conn, e := rpc.DialHTTP("tcp", "localhost:"+surfClient.ServerAddr)
	if e != nil {
		return e
	}

	e = conn.Call("Surfstore.GetFileInfoMap", succ, serverFileInfoMap)

	if e != nil {
		conn.Close()
		return e
	}

	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
	conn, e := rpc.DialHTTP("tcp", "localhost:"+surfClient.ServerAddr)
	if e != nil {
		return e
	}

	e = conn.Call("Surfstore.UpdateFile", fileMetaData, latestVersion)
	if e != nil {
		conn.Close()
		return e
	}

	return nil
}

var _ Surfstore = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		ServerAddr: hostPort,
		BaseDir:    baseDir,
		BlockSize:  blockSize,
	}
}
