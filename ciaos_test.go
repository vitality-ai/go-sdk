package ciaos_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ciaos "github.com/vitality-ai/go-sdk"
	"github.com/vitality-ai/go-sdk/utils/handlers"
)

// testConfig returns a sample configuration for
// testing purpose.This configuration includes
// a mock APIURL, a test UserId and a test UserAccessKey.
func testConfig() *ciaos.Config {
	return &ciaos.Config{
		APIURL:        "http://test-api.com",
		UserId:        "testuser",
		UserAccessKey: "testaccesskey",
	}
}

// TestPutSuccess tests the successful PUT request
// to upload a file to the server. It checks that the file is written
// to a temporary file [tmpFile], the server recieves the correct
// file name and header, and the response is as expected.
func TestPutSuccess(t *testing.T) {
	testData := []byte("test data")
	tmpFile := "test.txt"

	tmpFilePath := tmpFile
	err := os.WriteFile(tmpFilePath, testData, 0644)
	if err != nil {
		t.Fatalf("Failed to write to temporary file: %v", err)
	}
	defer os.Remove(tmpFilePath)

	// Stimulate the mock server for request handling.
	// In particular it validates the file name, checks the
	// header and responds with the proper response.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileName := filepath.Base(r.URL.Path)

		if fileName != tmpFile {
			t.Errorf("Unexpected file name in URL path: %s", fileName)
		}

		expectedHeaders := "testuser"

		if r.Header.Get("User") != expectedHeaders {
			t.Errorf("Expected header User: %s, got: %s", expectedHeaders, r.Header.Get("User"))
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Data uploaded successfully: key = test.txt"))
	}))
	defer mockServer.Close()

	// Configure the client with the mock servers URL.
	cfg := testConfig()
	cfg.APIURL = mockServer.URL
	ciaos := cfg
	fileName := filepath.Base(tmpFilePath)

	// Performs Put request to upload the file
	// checks if the response status code of 200
	// then reads the response body and verify the
	// response content matches the expected success message
	response, err := ciaos.Put(fileName, tmpFilePath)

	if err != nil {
		t.Fatalf("Failed to perform PUT request: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	defer response.Body.Close()

	expectedResponse := "Data uploaded successfully: key = test.txt"
	if string(body) != expectedResponse {
		t.Errorf("Expected response: %s, got: %s", expectedResponse, string(body))
	}
}

// TestPutBinarySucceess tests the successful PUT request
// to upload binary data to the server. It checks that the
// server recieves the correct URL path, headers, and responds
// with the expected status and body.
func TestPutBinarySuccess(t *testing.T) {
	key := "testkey"
	dataList := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
	}

	// Stimulate the mock server for request handling.
	// In particular it validates the file name, checks the header,
	// and responds with either a success message or an error.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/put/"+key {
			t.Errorf("Unexpected URL path: %s, expected: /put/%s", r.URL.Path, key)
		}

		expectedHeader := "testuser"
		if r.Header.Get("User") != expectedHeader {
			t.Errorf("Expected header 'User' to be %s, but got %s", expectedHeader, r.Header.Get("User"))
		}

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("Data uploaded successfully: key = " + key))
		if err != nil {
			t.Fatalf("Failed to write response: %v", err)
		}
	}))
	defer mockServer.Close()

	// Configure the client with the mock servers URL.
	cfg := testConfig()
	cfg.APIURL = mockServer.URL
	ciaos := cfg

	// Performs Put request to upload the Binary data
	// checks if the response status code of 200
	// then reads the response body and verify the
	// response content matches the expected success message
	response, err := ciaos.PutBinary(key, dataList)
	if err != nil {
		t.Fatalf("Failed to perform PUT binary request: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	defer response.Body.Close()

	expectedResponse := "Data uploaded successfully: key = " + key
	if string(body) != expectedResponse {
		t.Errorf("Expected response: %s, got: %s", expectedResponse, string(body))
	}
}

// TestUpdateKeySuccess tests the successful key update functionality
// Ensures the proper construction of URL with the old and new key
// and wheather the req contains the expected headers and handles the
// successful key updation with the proper response from the server.
func TestUpdateKeySuccess(t *testing.T) {
	oldKey := "oldkey"
	newKey := "newkey"

	// Stimulate the mock server for request handling.
	// It particularly validates the request URL path contains
	// both old and new keys and proper headers and writes the
	// proper return reponses with status code.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/update_key/"+oldKey+"/"+newKey) {
			t.Errorf("Unexpected URL path: %s", r.URL.Path)
		}

		expectedHeader := "testuser"
		if r.Header.Get("User") != expectedHeader {
			t.Errorf("Expected header 'User' to be %s, but got %s", expectedHeader, r.Header.Get("User"))
		}

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("Key updated successfully"))
		if err != nil {
			t.Fatalf("Failed to write response: %v", err)
		}
	}))
	defer mockServer.Close()

	// Configure the client with mock servers URL.
	cfg := testConfig()
	cfg.APIURL = mockServer.URL
	ciaos := cfg

	// Perform UpdateKey request with old and newKey
	// It verifies the key update operation wheather
	// response matches the expected success message.
	response, err := ciaos.UpdateKey(oldKey, newKey)
	if err != nil {
		t.Fatalf("Failed to perform key updation: %v", err)
	}

	expectedResponse := "Key updated successfully"
	if response != expectedResponse {
		t.Errorf("Expected response: %s, got: %s", expectedResponse, response)
	}

}

// TestUpadateSuccess tests the successful execution of
// update request. It particularly verifies the server recieves
// correct URL path and headers	and responds with the
// expected status and body.
func TestUpdateSuccess(t *testing.T) {
	key := "testkey"
	dataList := [][]byte{
		[]byte("newdata1"),
		[]byte("newdata2"),
	}

	// Stimulate the mock server for update request handling
	// It particularly validate the URL path includes the
	//correct path and checks for the "User" header in request
	// and then responds with the proper response.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/update/"+key) {
			t.Errorf("Unexpected URL path: %s", r.URL.Path)
		}

		expectedHeader := "testuser"
		if r.Header.Get("User") != expectedHeader {
			t.Errorf("Expected header 'User' to be %s, but got %s", expectedHeader, r.Header.Get("User"))
		}

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("Data updated successfully"))
		if err != nil {
			t.Fatalf("Failed to write response: %v", err)
		}
	}))
	defer mockServer.Close()

	// Configures the cient with the mock servers URL.
	cfg := testConfig()
	cfg.APIURL = mockServer.URL
	ciaos := cfg

	// Performs the update request with the key and
	// dataList where it reads the response body and
	// validates the response by content matching and
	// status code and then finally return the response
	response, err := ciaos.Update(key, dataList)
	if err != nil {
		t.Fatalf("Failed to perform update request: %v", err)
	}

	if response.StatusCode != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	defer response.Body.Close()

	expectedResponse := "Data updated successfully"
	if string(body) != expectedResponse {
		t.Errorf("Expected response: %s, got: %s", expectedResponse, string(body))
	}
}

// TestAppedSuccess tests the successful execution of an
// apppend request where it verifies that server recieves the
// correct URL and responds with expected status and body.
func TestAppendSuccess(t *testing.T) {
	key := "testkey"
	dataList := [][]byte{
		[]byte("appenddata1"),
		[]byte("appenddata2"),
	}

	// Stimulate the mockServer to handle the append request
	// where it validates the URL path include the correct
	// append key and header with "User" in the request.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/append/"+key) {
			t.Errorf("Unexpected URL path: %s", r.URL.Path)
		}

		expectedHeader := "testuser"
		if r.Header.Get("User") != expectedHeader {
			t.Errorf("Expected header 'User' to be %s, but got %s", expectedHeader, r.Header.Get("User"))
		}

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("Data appended successfully"))
		if err != nil {
			t.Fatalf("Failed to write response: %v", err)
		}
	}))
	defer mockServer.Close()

	// Configure the client with mock servers URL.
	cfg := testConfig()
	cfg.APIURL = mockServer.URL
	ciaos := cfg

	// Perfoms the append request with key and dataList
	// where it verifies the response content matches
	// to the expected response.
	response, err := ciaos.Append(key, dataList)
	if err != nil {
		t.Fatalf("Failed to perform append request: %v", err)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	defer response.Body.Close()

	expectedResponse := "Data appended successfully"
	if string(body) != expectedResponse {
		t.Errorf("Expected response: %s, got: %s", expectedResponse, string(body))
	}
}

// TestGetSuccess tests the successful retrieval of
// data. here it mainly checks wheather server recieves the
// correct URL path, headers, parsing of FlatBuffer response
// and then finally returns the response.
func TestGetSuccess(t *testing.T) {
	key := "testkey"
	expectedData := [][]byte{
		[]byte("data1"),
		[]byte("data2"),
	}

	// Stimulate the mockServer for request handing.
	// In particular it validates the request URL path,
	// "User" header and converts the expected data to the
	// flatBuffer format and writes the flatBuffer data as response.
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/get/"+key) {
			t.Errorf("Unexpected URL path: %s, expected to contain: /get/%s", r.URL.Path, key)
		}

		expectedHeader := "testuser"
		if r.Header.Get("User") != expectedHeader {
			t.Errorf("Expected header 'User' to be %s, but got %s", expectedHeader, r.Header.Get("User"))
		}

		w.WriteHeader(http.StatusOK)
		flatbufferData, err := handlers.CreateFlatBuffer(expectedData)
		if err != nil {
			t.Fatalf("Failed to create FlatBuffer data: %v", err)
		}
		_, err = w.Write(flatbufferData)
		if err != nil {
			t.Fatalf("Failed to write response: %v", err)
		}
	}))
	defer mockServer.Close()

	// Configure the client with mockServers URL.
	cfg := testConfig()
	cfg.APIURL = mockServer.URL
	ciaos := cfg

	// Performs Get request with the testKey and verifies
	// the number of results matches the expected data
	// and Compare each result with the expected data.
	result, err := ciaos.Get(key)
	if err != nil {
		t.Fatalf("Failed to perform GET request: %v", err)
	}

	if len(result) != len(expectedData) {
		t.Fatalf("Expected %d results, got %d", len(expectedData), len(result))
	}
	for i, res := range result {
		if string(res) != string(expectedData[i]) {
			t.Errorf("Mismatch at index %d: expected %s, got %s", i, expectedData[i], res)
		}
	}
}

// TestEmptyUserId tests the behaviour of the system
// when the "UserID" in the configuration is empty.
// Ensures the systen returns the appropriate error msg.
func TestEmptyUserId(t *testing.T) {
	// Creates the configuration with an empty UserId.
	// Initialize the client with the invalid configuration.
	cfg := testConfig()
	cfg.UserId = ""
	ciaos := cfg

	_, err := ciaos.Put("testfile", "test.txt")
	if err == nil || err.Error() != "user id must not be empty" {
		t.Fatalf("Expected error: 'User id must not be empty', got: %v", err)
	}
}

// TestEmptyApiUrl tests the behaviour of the system
// when the "APIURL" in the configuration is empty.
// Ensures the systen returns the appropriate error msg.
func TestEmptyApiUrl(t *testing.T) {
	// Creates the configuration with an empty APIURL.
	// Initialize the client with the invalid configuration.
	cfg := testConfig()
	cfg.APIURL = ""
	ciaos := cfg

	_, err := ciaos.Put("testfile", "test.txt")
	if err == nil || err.Error() != "api url must not be empty" {
		t.Fatalf("Expected error: 'api url must not be empty', got: %v", err)
	}
}

// TestPutFileNotFound tests the behaviour of the PUT method,
// when attempting to upload the non-extising file.
func TestPutFileNotFound(t *testing.T) {
	// Non-existing filePath
	nonExistentFile := "nonExistentFile.txt"

	// Creates the test configuration and attempts
	// to perform the PUT request with non-existing file
	// and verify the server throws an appropriate error msg.
	cfg := testConfig()
	ciaos := cfg
	_, err := ciaos.Put(nonExistentFile, nonExistentFile)
	if err == nil || err.Error() != fmt.Sprintf("file not found: %s", nonExistentFile) {
		t.Fatalf("Expected error: 'file not found: %s', got: %v", nonExistentFile, err)
	}
}

// TestPutEmptyFilePath tests the Put method's
// handling of an empty file path
func TestPutEmptyFilePath(t *testing.T) {
	cfg := testConfig()
	ciaos := cfg

	// Attempt to put a file with an empty path
	// Verifies that an error is returned with the correct error msg
	_, err := ciaos.Put("", "")
	if err == nil || err.Error() != "file_path cannot be empty or None" {
		t.Fatalf("Expected error: 'file_path cannot be empty or None', got: %v", err)
	}
}
