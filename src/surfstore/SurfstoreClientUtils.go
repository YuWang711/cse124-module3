package surfstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"crypto/sha256"
	"bufio"
	"strings"
	"strconv"
	"io"
)

/*
Implement the logic for a client syncing with the server here.
*/
func ClientSync(client RPCClient) {
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Print(err)
	}
	if _,err := os.Stat(client.BaseDir + "/index.txt"); os.IsNotExist(err) {
		f,_ := os.Create(client.BaseDir + "/index.txt")
		f.Close()
	}

	//Getting Local Index
	index_file, err := os.Open(client.BaseDir + "/index.txt")
	reader := bufio.NewReader(index_file)
	temp_FileMetaMap := make(map[string]FileMetaData)

	for{
		var isPrefix bool
		var line string
		var l []byte
		for {
			l,isPrefix,err = reader.ReadLine()
		//	log.Printf(string(l))
			line = line + string(l)
			if !isPrefix {
				break
			}
			if err != nil && err == io.EOF {
				break
			}
		}
		if line != "" {
			log.Print(line)
			var new_File_Meta_Data FileMetaData
			new_File_Meta_Data = handleIndex(string(line))
			temp_FileMetaMap[new_File_Meta_Data.Filename] = new_File_Meta_Data
		}
		if err == io.EOF{
			break
		}
	}
	index_file.Close()
	//Getting Remote Index
	remote_FileMetaMap := make(map[string]FileMetaData)
	var success bool
	client.GetFileInfoMap(&success, &remote_FileMetaMap)
	//PrintMetaMap(remote_FileMetaMap)
	//Sorting Local Index
	//Client, files, 
	//Handle Deleted Files
	Handle_Deleted_File(client, temp_FileMetaMap, files)

	Local_Mod_FileMetaMap,Local_new_FileMetaMap,Local_No_Mod_FileMetaMap := CheckForNewChangedFile(client,files, temp_FileMetaMap)
	for index,element := range Local_new_FileMetaMap {
		//Case 1 : new file was created locally that WAS NOT on the server
	//	log.Print("New File")
		if _, ok := remote_FileMetaMap[index]; !ok {
			handleNewFile(client,element)
		} else {
		//Case 2: new file was created locally but it WAS on the server
			if Local_new_FileMetaMap[index].Version < remote_FileMetaMap[index].Version {
				//Update it to remote version
				UpdateLocal(client, remote_FileMetaMap[index])
			} else if Local_new_FileMetaMap[index].Version == remote_FileMetaMap[index].Version {
				//Sync changes to the cloud
				//Update Remote File
				UpdateRemote(client, Local_new_FileMetaMap[index])
			}
		}
	}
	for index,element := range Local_No_Mod_FileMetaMap {
	//	log.Print("No Mod")
		//Case 1 : if local no modification file WAS NOT on the server
		if _, ok := remote_FileMetaMap[index]; !ok {
			handleNewFile(client,element)
		} else {
		//Case 2: if Local Modification file WAS on the server
	//		log.Print("CLIENT - REMOTE VERSION : ", remote_FileMetaMap[index].Version)
			if Local_No_Mod_FileMetaMap[index].Version < remote_FileMetaMap[index].Version {
				//Update it to remote version
				UpdateLocal(client, remote_FileMetaMap[index])
			}
		}
	}
	for index,element := range Local_Mod_FileMetaMap {
	//	log.Print("Mod")
		//Case 1 : if local modified file WAS NOT on the server
		if _, ok := remote_FileMetaMap[index]; !ok {
	//		log.Print("NOT IN SERVER")
			handleNewFile(client,element)
		} else {
		//Case 2: if Local Modified file WAS on the server
	//		log.Print("IN SERVER")
			if Local_Mod_FileMetaMap[index].Version < remote_FileMetaMap[index].Version {
				//Update it to remote version
	//			log.Print("SERVER VERSION IS HIGHER")
				//Lost the race
				UpdateLocal(client, remote_FileMetaMap[index])
			} else if Local_Mod_FileMetaMap[index].Version == remote_FileMetaMap[index].Version {
				//Sync changes to the cloud
				//Check if file is the same.
				//Don't touch if it is
				//Update Remote File, version += 1
	//			log.Print("VERSION ARE EQUAL")
				UpdateRemote(client, Local_Mod_FileMetaMap[index])
			}
		}
	}
	client.GetFileInfoMap(&success, &remote_FileMetaMap)
	//PrintMetaMap(remote_FileMetaMap)
	UpdateIndex(client,remote_FileMetaMap)
	//Check for deleted files
}

/*
Helper function to print the contents of the metadata map.
*/
func PrintMetaMap(metaMap map[string]FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version, filemeta.BlockHashList)
	}

	fmt.Println("---------END PRINT MAP--------")

}

func CheckForNewChangedFile(client RPCClient, files []os.FileInfo,temp_FileMetaMap map[string]FileMetaData) (map[string]FileMetaData, map[string]FileMetaData, map[string]FileMetaData) {
	Changed := make(map[string]FileMetaData)
	New := make(map[string]FileMetaData)
	NoMod := make(map[string]FileMetaData)
	for _, f := range files {
		if f.Name() == "index.txt" {
			continue
		}
		var file_size int64
		file_size = f.Size()
		nblocks := (file_size / int64(client.BlockSize)) + 1
		open_file, err := os.Open(client.BaseDir + "/" + f.Name())
		if err != nil{
			log.Print(err)
		}
		//If file is found in index.txt
		if local, ok := temp_FileMetaMap[f.Name()]; ok{
			//Check if hashlist is fine
			Code,hash_list := handleBlocks(nblocks,open_file, client, local)
			if Code == 1 {
				var new_FileMetaData FileMetaData
				new_FileMetaData.Filename = f.Name()
				new_FileMetaData.Version = temp_FileMetaMap[f.Name()].Version
				for _,element := range hash_list{
					new_FileMetaData.BlockHashList = append(new_FileMetaData.BlockHashList, element)
				}
				Changed[f.Name()] = new_FileMetaData
//				log.Println("Changes Detected in ", f.Name())
			} else {
				var new_FileMetaData FileMetaData
				new_FileMetaData.Filename = f.Name()
				new_FileMetaData.Version = temp_FileMetaMap[f.Name()].Version
				for _,element := range hash_list{
					new_FileMetaData.BlockHashList = append(new_FileMetaData.BlockHashList, element)
				}
				NoMod[f.Name()] = new_FileMetaData
//				log.Println("No Changes Detected in ", f.Name())
			}
		} else {
			//If file is not found in index.txt
			//Situation 1/2
			//Still wrong need update
//			log.Print("Case 1/2")
			index_file, err := os.OpenFile(client.BaseDir + "/index.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
			if err != nil {
				log.Print(err)
			}
			_,hash_list := handleBlocks(nblocks,open_file, client, local)
			var new_FileMetaData FileMetaData
			new_FileMetaData.Filename = f.Name()
			new_FileMetaData.Version = 1

			var hash_list_string string
			for _,hash_string := range hash_list {
				hash_byte := []byte(hash_string)
				for _,element := range hash_byte {
					hash_list_string += strconv.Itoa(int(element)) + " "
				}
				new_FileMetaData.BlockHashList = append(new_FileMetaData.BlockHashList, hash_string)
			}
			version := strconv.Itoa(new_FileMetaData.Version)
			New[f.Name()] = new_FileMetaData
//			log.Print("updating file with :" ,hash_list_string)
			_,err = index_file.WriteString(new_FileMetaData.Filename + "," + version + "," + hash_list_string + "\n")

			if err != nil {
				log.Print(err)
			}
			index_file.Close()
		}
		open_file.Close()
	}
	return Changed, New, NoMod
}

func handleBlocks(nblocks int64, open_file *os.File, client RPCClient, local FileMetaData) (int, []string){
	Code := 0
	hash_list_string := make([]string, int(nblocks))
//	log.Print("this file has ", nblocks , " number of blocks")
	for i := 0; int64(i) < nblocks; i++ {
		var new_block Block
		new_block.BlockData = make([]byte,client.BlockSize)
		num,_ := open_file.Read(new_block.BlockData)
		new_block.BlockSize = num
		h := sha256.New()
		h.Write(new_block.BlockData)
		h_val := h.Sum(nil)
//		log.Print("Actual Text hash ", h_val)
		var h_val_s string
		for _,element := range h_val{
			h_val_s += string(element)
		}
		if len(local.BlockHashList) >= (i+1) {
//			log.Print("HANDLE BLOCKS- blockhashlist ", local.BlockHashList[i])
//			log.Print("HANDLE BLOCKS- H_VAL_S ", h_val_s)
			if h_val_s != local.BlockHashList[i] {
				Code = 1
			}
		}
		hash_list_string[i] = string(h_val)
	}
	return Code, hash_list_string
}


func handleIndex(file_info string) (FileMetaData){
	s := strings.Split(file_info, ",")
	var new_FileMetaData FileMetaData
	new_FileMetaData.Filename = s[0]
	s_1,_ := strconv.ParseInt(s[1], 10, 0)
	new_FileMetaData.Version = int(s_1)
	//Need fixed
//	log.Print("HANDLE INDEX - PRINTING HASH LIST")
	s_2 := strings.Split(s[2], " ")
	var hash_string string
	for i:= 1; i <= len(s_2);i++ {
		int_form,_ := strconv.Atoi(s_2[i-1])
		byte_form := byte(int_form)
		hash_string += string(byte_form) 
		//log.Print(hash_string)
		if(i%32) == 0{
			new_FileMetaData.BlockHashList = append(new_FileMetaData.BlockHashList, hash_string)
			hash_string = ""
		}
	}
	//log.Print(new_FileMetaData.Filename)
	return new_FileMetaData
}


func handleNewFile(client RPCClient, new_file FileMetaData) {
	var file_size int64
	f,err := os.Stat(client.BaseDir+ "/" + new_file.Filename)
//	log.Print("name :", new_file.Filename)
	file_size = f.Size()
	nblocks := (file_size / int64(client.BlockSize)) + 1
	open_file, err := os.Open(client.BaseDir + "/" + f.Name())
	if err != nil {
		log.Print("Handle New File: ", err)
	}
	for i:= 0; int64(i) < nblocks; i++ {
		var new_block Block
		var success bool
		new_block.BlockData = make([]byte,client.BlockSize)
		num,_ := open_file.Read(new_block.BlockData)
		new_block.BlockSize = num
		err = client.PutBlock(new_block, &success)
		if err != nil {
			log.Print(err)
		}
	}
//	log.Print("CLIENT-UTIL VERSION:" ,new_file.Version)
	err = client.UpdateFile(&new_file, &new_file.Version)
//	log.Print("CLIENT-UTIL VERSION:" ,new_file.Version)
	if err != nil {
		log.Print(err)
	}
//	log.Print("CLIENT-UTIL CALLED UPDATE FILE - HANDLE NEW FILE")
	open_file.Close()
}

func UpdateIndex(client RPCClient, remote_file map[string]FileMetaData){
	//log.Print("Updating File Index.txt")
	err := os.Truncate(client.BaseDir+"/index.txt", 0)
	update_file, err := os.OpenFile(client.BaseDir + "/index.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
	if err != nil {
		log.Print(err)
	}
	for _, element := range remote_file {
		var hash_list_string string
		for _,hash_string := range remote_file[element.Filename].BlockHashList {
			hash_byte := []byte(hash_string)
			for _,element := range hash_byte {
				hash_list_string += strconv.Itoa(int(element)) + " "
			}
		}
		version := strconv.Itoa(element.Version)
		_,err := update_file.WriteString(element.Filename + "," + version + "," + hash_list_string + "\n")
		if err != nil{
			log.Print(err)
		}
	}
	update_file.Close()
}

func UpdateLocal(client RPCClient, remote_file FileMetaData){
	//log.Print("UPDATE LOCAL")
	err := os.Truncate(client.BaseDir+"/"+remote_file.Filename, 0)
	update_file, err := os.OpenFile(client.BaseDir + "/"+remote_file.Filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
	if err != nil {
		log.Print("Update file: ", err)
	}
	for _, element := range remote_file.BlockHashList{
	//	log.Print("block hash list :" ,element)
		var block Block
		client.GetBlock(element, &block)
//		log.Print("Retrieved Block :", string(block.BlockData))
//		log.Print("Block Size ", block.BlockSize)
		next_string := make([]byte,block.BlockSize)
		for i:=0; i < block.BlockSize;i++ {
			next_string[i] = block.BlockData[i]
		}
		_, err := update_file.WriteString(string(next_string))
		if err != nil {
			log.Print(err)
		}
	}
	update_file.Close()
}

func UpdateRemote(client RPCClient, local_file FileMetaData) {
	var file_size int64
	f,err := os.Stat(client.BaseDir+ "/" + local_file.Filename)
	file_size = f.Size()
	nblocks := (file_size / int64(client.BlockSize)) + 1
	open_file, err := os.Open(client.BaseDir + "/" + f.Name())
	if err != nil {
		log.Print("Handle New File: ", err)
	}
	for i:= 0; int64(i) < nblocks; i++ {
		var new_block Block
		var success bool
		new_block.BlockData = make([]byte,client.BlockSize)
		num,_ := open_file.Read(new_block.BlockData)
		new_block.BlockSize = num
		err = client.PutBlock(new_block, &success)
		if err != nil {
			log.Print(err)
		}
	}
	local_file.Version = local_file.Version + 1
	var file_latest_version int
	e := client.UpdateFile(&local_file, &file_latest_version)
	open_file.Close()
	if e != nil{
		log.Print(e)
		remote_FileMetaMap := make(map[string]FileMetaData)
		var success bool
		client.GetFileInfoMap(&success, &remote_FileMetaMap)
		UpdateLocal(client, remote_FileMetaMap[local_file.Filename])
	}
}

func Handle_Deleted_File(client RPCClient, temp_FileMetaMap map[string]FileMetaData, files []os.FileInfo) {
	err := os.Truncate(client.BaseDir+"/index.txt", 0)
	index_file, err := os.OpenFile(client.BaseDir + "/index.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY,0644)
//	log.Print("Deleting File")
	found := 0
	if err != nil {
		log.Print(err)
	}
	for _,element := range temp_FileMetaMap {
		for _,file := range files {
			if element.Filename == file.Name() {
				found = 1
				var hash_list_string string
				for _,hash_string := range temp_FileMetaMap[element.Filename].BlockHashList {
					hash_byte := []byte(hash_string)
					for _,element := range hash_byte {
						hash_list_string += strconv.Itoa(int(element)) + " "
					}
				}
				version := strconv.Itoa(element.Version)
				_,err := index_file.WriteString(element.Filename + "," + version + "," + hash_list_string)
				if err != nil {
					log.Print(err)
				}
				break
			}
		}
		if found == 0 {
			version := strconv.Itoa(element.Version)
			index_file.WriteString(element.Filename + "," + version + ",0")
		}
		found = 0
	}
	index_file.Close()
}
