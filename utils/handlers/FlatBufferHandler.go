package handlers

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/vitality-ai/go-sdk/utils/flatbuffer"
)

func CreateFlatBuffer(dataList [][]byte) ([]byte, error) {
	// 	Serializes a list of byte arrays into FlatBuffers binary format.
	//
	// 	Args:
	//     	dataList ([][]byte): List of file data in bytes.
	//
	// 	Returns:
	//     	([]byte, error): Serialized FlatBuffers data, or an error if one occurs.

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Error creating FlatBuffers data:", r)
		}
	}()

	builder := flatbuffers.NewBuilder(0)

	fileDataOffsets := make([]flatbuffers.UOffsetT, len(dataList))

	for i, data := range dataList {
		dataOffset := builder.CreateByteVector(data)
		flatbuffer.FileDataStart(builder)
		flatbuffer.FileDataAddData(builder, dataOffset)
		fileDataOffsets[i] = flatbuffer.FileDataEnd(builder)
	}

	flatbuffer.FileDataListStartFilesVector(builder, len(fileDataOffsets))
	for i := len(fileDataOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(fileDataOffsets[i])
	}

	filesVector := builder.EndVector(len(fileDataOffsets))
	flatbuffer.FileDataListStart(builder)
	flatbuffer.FileDataListAddFiles(builder, filesVector)
	fileDataListOffset := flatbuffer.FileDataListEnd(builder)
	builder.Finish(fileDataListOffset)

	fmt.Printf("Created FlatBuffer with %d files\n", len(dataList))

	return builder.Bytes[builder.Head():], nil

}

func GetDataVector(fileDataFBObj *flatbuffer.FileData) ([]byte, error) {
	//	Extracts the byte array from a FileData FlatBuffer object.
	//
	//	Args:
	//  file_data_fb_obj: A FileData FlatBuffer object.
	//
	//	Returns:
	//    	bytes or None: The extracted byte array or None if not present.

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Error extracting data vector: %v\n", r)
		}
	}()

	if fileDataFBObj == nil {
		return nil, fmt.Errorf("nil FileData object")
	}

	dataLen := fileDataFBObj.DataLength()
	if dataLen == 0 {
		return nil, fmt.Errorf("no data found")
	}

	data := make([]byte, dataLen)
	for i := 0; i < dataLen; i++ {
		data[i] = fileDataFBObj.Data(i)
	}

	return data, nil
}

func ParseFlatBuffer(flatBufferData []byte) ([][]byte, error) {
	// 	Deserializes FlatBuffers data and extracts file byte arrays.
	//
	//	Args:
	//    	flatbuffer_data (bytes): Serialized FlatBuffers data.
	//
	//	Returns:
	//   	List[bytes]: List of file data in bytes.
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Error parsing FlatBuffers data:", r)
		}
	}()

	fileDataList := [][]byte{}

	fileDataListFB := flatbuffer.GetRootAsFileDataList(flatBufferData, 0)
	numFiles := fileDataListFB.FilesLength()
	fmt.Printf("Number of files: %d\n", numFiles)

	for i := 0; i < numFiles; i++ {
		var fileDataFBObj flatbuffer.FileData
		fileDataListFB.Files(&fileDataFBObj, i)
		dataBytes, err := GetDataVector(&fileDataFBObj)
		if err != nil {
			fmt.Printf("No data for file %d: %v\n", i, err)
			continue
		}

		fileDataList = append(fileDataList, dataBytes)
		fmt.Printf("Retrieved file %d, size %d bytes\n", i, len(dataBytes))
	}

	fmt.Printf("Parsed %d files from FlatBuffer\n", len(fileDataList))
	return fileDataList, nil
}
