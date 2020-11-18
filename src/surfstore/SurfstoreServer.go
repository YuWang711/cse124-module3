package surfstore

import (
	"log"
//	"net"
	"net/http"
	"net/rpc"
//	"sync"
)

type Server struct {
	BlockStore BlockStoreInterface
	MetaStore  MetaStoreInterface
	//Mutex      *sync.Mutex
}

func (s *Server) GetFileInfoMap(succ *bool, serverFileInfoMap *map[string]FileMetaData) error {
//	s.Mutex.Lock()
	defer func(){
//		s.Mutex.Unlock()
		if r := recover(); r != nil{
			*succ = false
	//		log.Print("GETFILEINFOMAP - SERVER success :", *succ)
		} else {
			*succ = true
	//		log.Print("GETFILEINFOMAP - SERVER success :", *succ)
		}
	}()
	s.MetaStore.GetFileInfoMap(succ, serverFileInfoMap)

	return nil
}

func (s *Server) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) error {
//	s.Mutex.Lock()
	defer func(){
//		s.Mutex.Unlock()
		if r := recover(); r != nil{
	//		log.Print("UPDATEFILE - SERVER")
		} else {
	//		log.Print("UPDATEFILE - SERVER")
		}
	}()
//	log.Print("UPDATEFILE - SERVER")
//	log.Print("Filename : ", fileMetaData.Filename)
//	log.Print("Version : ", *latestVersion)
	s.MetaStore.UpdateFile(fileMetaData,latestVersion)
	return nil
}

func (s *Server) GetBlock(blockHash string, blockData *Block) error {
//	s.Mutex.Lock()
	defer func(){
//		s.Mutex.Unlock()
		if r := recover(); r != nil{
			log.Print("Recovered in GetBlock()")
		} else {
		}
	}()
	err := s.BlockStore.GetBlock(blockHash, blockData)
	if err != nil {
		log.Print(err)
	}
	return nil
}

func (s *Server) PutBlock(blockData Block, succ *bool) error {
//	s.Mutex.Lock()
	defer func(){
//		s.Mutex.Unlock()
		if r := recover(); r != nil{
			*succ = false
	//		log.Print("PUTBLOCK - SERVER success:", *succ)
		} else {
			*succ = true
	//		log.Print("PUTBLOCK - SERVER success:", *succ)
		}
	}()
//	log.Print("BlockByte : " ,blockData.BlockData)
//	log.Print("BlockSize: " ,blockData.BlockSize)
	err := s.BlockStore.PutBlock(blockData, succ)
	if err != nil {
		log.Print(err)
		return err
	}
	return nil
}

func (s *Server) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
//	s.Mutex.Lock()
	defer func(){
//		s.Mutex.Unlock()
		if r := recover(); r != nil{
			log.Print("Recovered in HasBlocks()")
		} else {
		}
	}()
	return nil
}

// This line guarantees all method for surfstore are implemented
var _ Surfstore = new(Server)

func NewSurfstoreServer() Server {
	blockStore := BlockStore{BlockMap: map[string]Block{}}
	metaStore := MetaStore{FileMetaMap: map[string]FileMetaData{}}
//	mutex := &sync.Mutex{}
	return Server{
		BlockStore: &blockStore,
		MetaStore:  &metaStore,
//		Mutex: mutex,
	}
}

func ServeSurfstoreServer(hostAddr string, surfstoreServer Server) error {

	rpc.RegisterName("Surfstore",&surfstoreServer)
	rpc.HandleHTTP()
	log.Print("Starting server at Host Address: ", hostAddr)
//	_, e := net.Listen("tcp", hostAddr)
//	if e != nil {
//		log.Print("listen error:", e)
//	}
	return http.ListenAndServe(hostAddr,nil)

}
