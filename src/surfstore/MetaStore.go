package surfstore

import (
	"log"
)
type MetaStore struct {
	FileMetaMap map[string]FileMetaData
}

type MyError struct{}

func (m *MetaStore) GetFileInfoMap(_ignore *bool, serverFileInfoMap *map[string]FileMetaData) error {
	log.Print("In MetaStore")
	for _,element := range m.FileMetaMap {
		log.Print("Filename: ",element.Filename)
		log.Print("File Version: ",element.Version)
	}
	*serverFileInfoMap = m.FileMetaMap
	return nil
}

func (m *MetaStore) UpdateFile(fileMetaData *FileMetaData, latestVersion *int) (err error) {

	filename := fileMetaData.Filename
	if _,ok := m.FileMetaMap[filename]; ok {
		if (m.FileMetaMap[filename].Version+1) == fileMetaData.Version {
			m.FileMetaMap[filename] = *fileMetaData
			*latestVersion = m.FileMetaMap[filename].Version
			log.Print("METASTORE - UPDATEFILE: ", filename)
			return nil
		} else {
			return &MyError{}
		}
	} else {
		log.Print("METASTORE - UPDATEFILE: ", filename)
		m.FileMetaMap[filename] = *fileMetaData
		*latestVersion = m.FileMetaMap[filename].Version
		return nil
	}
}

func (m *MyError) Error() string{
	return "Update File Version was smaller than Remove File Version"
}

var _ MetaStoreInterface = new(MetaStore)
