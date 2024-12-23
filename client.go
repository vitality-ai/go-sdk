package ciaos

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"

	flatbufferHandler "github.com/vitality-ai/go-sdk/utils/handlers"
)

// Initializes Ciaos client with configuration parameters.
// It particularly validates the provided struct and returns
// a pointer to it if valid, or an error for required missing fields.

// Parameters:
//  config (Config): The configuration object to validate.

// Returns:
// (*Config): A pointer to the validated configuration object.
// (error): An error if validation fails.
func Ciaos(config Config) (*Config, error) {

	if config.UserId == "" {
		return nil, fmt.Errorf("user id must not be empty")
	}

	if config.APIURL == "" {
		return nil, fmt.Errorf("api url must not be empty")
	}

	return &config, nil
}

// Put uploads a file to a server, storing it with the given key.
// If no key is provided, the base name of the filePath is used as
// the key.

// Note: Despite the name, this function uses the POST method,
// not the PUT method.

// Parameters:
//  filePath (string): The path to the file that needs to be uploaded.
//  key (string): The key under which the file will be stored.
//                If empty, defaults to the file's base name.

// Returns:
// (*http.Response): The HTTP response from the server.
// (error): An error if the operation fails at any step.
func (config *Config) Put(filePath string, key string) (*http.Response, error) {

	if config.UserId == "" {
		return nil, fmt.Errorf("user id must not be empty")
	}

	if config.APIURL == "" {
		return nil, fmt.Errorf("api url must not be empty")
	}

	if filePath == "" {
		return nil, fmt.Errorf("file_path cannot be empty or None")
	}
	if filePath == "" {
		return nil, fmt.Errorf("file_path cannot be empty")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("file not found: %s", filePath)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	// Creates the FlatBuffer from the files data.
	flatBufferData, err := flatbufferHandler.CreateFlatBuffer([][]byte{data})
	if err != nil {
		return nil, fmt.Errorf("failed to create FlatBuffer Data: %v", err)
	}

	// Creates an HTTP POST request to upload the data,
	// adds the UserId in the header and execute the req.
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/put/%s", config.APIURL, key), bytes.NewReader(flatBufferData))
	if err != nil {
		return nil, fmt.Errorf("failed to create PUT request: %v", err)
	}
	req.Header.Set("User", config.UserId)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP PUT request failed: %v", err)
	}

	return resp, nil
}

// PutBinary uploads the binary data to the server with the specified key.

// Note: Despite the name, this function uses the POST method,
// not the PUT method.

// Parameters:
//  key (string): The key under which the binary data will be stored.
//  dataList ([][]byte): A list of byte slices containing the binary data to be uploaded.

// Returns:
// (*http.Response): The HTTP response from the server.
// (error): An error if the operation fails, including issues with FlatBuffer creation or the HTTP request.
func (config *Config) PutBinary(key string, dataList [][]byte) (*http.Response, error) {

	// Converts the binary dataList into a FlatBuffer format.
	flatBufferData, err := flatbufferHandler.CreateFlatBuffer(dataList)
	if err != nil {
		return nil, fmt.Errorf("failed to create FlatBuffer Data: %v", err)
	}

	// Creates an HTTP POST request to upload the FlatBuffer data,
	// adds the UserId in the header and execute the req.
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/put/%s", config.APIURL, key), bytes.NewReader(flatBufferData))
	if err != nil {
		return nil, fmt.Errorf("failed to create POST request: %v", err)
	}
	req.Header.Set("User", config.UserId)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP POST request failed: %v", err)
	}

	return resp, nil
}

// UpdateKey updates the key of an existing resource.

// Parameters:
//  oldKey (string): The current key of the resource to be updated.
//  newKey (string): The new key to assign to the resource.

// Returns:
// (string): The server's response body as a string, which may include confirmation or status details.
// (error): An error if the operation fails, including issues with the HTTP request or response processing.
func (config *Config) UpdateKey(oldKey string, newKey string) (string, error) {

	// Creates the POST request to update the key on the server.
	// adds the UserId to the req headers and execute the req.
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/update_key/%s/%s", config.APIURL, oldKey, newKey), nil)
	if err != nil {
		return "", fmt.Errorf("failed to create POST request: %v", err)
	}
	req.Header.Set("User", config.UserId)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP error during key update: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	return string(body), nil
}

// Update updates an existing resource with new binary data.

// Parameters:
// key (string): The key of the resource to be updated.
// dataList ([][]byte): A list of byte slices containing the binary data to update the resource.

// Returns:
// (*http.Response): The HTTP response from the server, which may include status or confirmation.
// (error): An error if the operation fails, including issues with FlatBuffer creation or the HTTP request.
func (config *Config) Update(key string, dataList [][]byte) (*http.Response, error) {

	// Converts the binary data list into a FlatBuffer format.
	flatBufferData, err := flatbufferHandler.CreateFlatBuffer(dataList)
	if err != nil {
		return nil, fmt.Errorf("failed to create FlatBuffer Data: %v", err)
	}

	// Creates an HTTP POST request to update the FlatBuffer data,
	// adds the UserId in the header and execute the req.
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/update/%s", config.APIURL, key), bytes.NewReader(flatBufferData))
	if err != nil {
		return nil, fmt.Errorf("failed to create PUT request: %v", err)
	}
	req.Header.Set("User", config.UserId)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP error during update: %v", err)
	}

	return resp, nil
}

// Append sends data to be appended to a key.

// Parameters:
// key: string identifier where the data will be appended
// dataList: [][]byte containing the data to be appended

// Returns:
// *http.Response: the server's response
// error: any error encountered during the operation
func (config *Config) Append(key string, dataList [][]byte) (*http.Response, error) {

	// Converts the binary dataList into a FlatBuffer format.
	flatBufferData, err := flatbufferHandler.CreateFlatBuffer(dataList)
	if err != nil {
		return nil, fmt.Errorf("failed to create FlatBuffer Data: %v", err)
	}

	// Creates an HTTP POST request to upload the FlatBuffer data,
	// adds the UserId in the header and execute the req.
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/append/%s", config.APIURL, key), bytes.NewReader(flatBufferData))
	if err != nil {
		return nil, fmt.Errorf("failed to create POST request: %v", err)
	}
	req.Header.Set("User", config.UserId)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP error during append: %v", err)
	}

	return resp, nil
}

// Delete removes the resource associated with the key

// Parameters:
// key (string): The key of the resource to be deleted.

// Returns:
// (*http.Response): The HTTP response from the server, which may include status or confirmation.
// (error): An error if the operation fails, including issues with creating or executing the DELETE request.
func (config *Config) Delete(key string) (*http.Response, error) {

	//Creates a DELETE request to remove the resource associated with the key.
	// adds the UserId to the req headers and execute the req.
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/delete/%s", config.APIURL, key), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create DELETE request: %v", err)
	}
	req.Header.Set("User", config.UserId)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP error during deletion: %v", err)
	}

	return resp, nil
}

// Get retrieves binary data and parses it from a FlatBuffer format.

// Parameters:
//  key (string): The key of the resource to be retrieved.

// Returns:
// ([][]byte): A list of byte slices containing the parsed binary data from the server.
// (error): An error if the operation fails, including issues with creating the request,
// retrieving the response, or parsing the FlatBuffer data.
func (config *Config) Get(key string) ([][]byte, error) {

	// Creates a GET request to retrieve the resource identified by the key.
	// adds the UserId to req headers and execute the request.
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/get/%s", config.APIURL, key), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GET request: %v", err)
	}
	req.Header.Set("User", config.UserId)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP error during retrieval: %v", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	// Parse the response body from a FlatBuffer format into a list of byte slices.
	// Return the parsed binary data.
	fileDataList, err := flatbufferHandler.ParseFlatBuffer(bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("error parsing FlatBuffer: %v", err)
	}
	return fileDataList, nil
}
