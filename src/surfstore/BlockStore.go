package surfstore

import(
	"crypto/sha256"
	"log"
)

type BlockStore struct {
	BlockMap map[string]Block
}

//Get blocks 
func (bs *BlockStore) GetBlock(blockHash string, blockData *Block) error {
	*blockData = bs.BlockMap[blockHash]
	log.Print("Block Stored ", string(bs.BlockMap[blockHash].BlockData))
	return nil
}

//Put blocks into BlockData
func (bs *BlockStore) PutBlock(block Block, succ *bool) error {

	defer func(){
		if r:= recover(); r != nil{
			*succ = false
			log.Print("PUTBLOCK - BLOCKSTORE success: ", *succ)
		} else {
			*succ = true
			log.Print("PUTBLOCK - BLOCKSTORE success: ", *succ)
		}
	}()
	h := sha256.New()
	h.Write(block.BlockData)
	h_val := h.Sum(nil)
	bs.BlockMap[string(h_val)] = block
	return nil
}

//Check if theres block
func (bs *BlockStore) HasBlocks(blockHashesIn []string, blockHashesOut *[]string) error {
	for _, element := range blockHashesIn {
		if _,ok := bs.BlockMap[element]; ok{
			*blockHashesOut = append(*blockHashesOut, element)
		}
	}
	return nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)
