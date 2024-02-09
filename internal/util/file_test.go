package util

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestListDir(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create some files and directories inside the temporary directory
	testFiles := []string{"file1.txt", "file2.txt", "file3.txt"}
	testDirs := []string{"dir1", "dir2"}
	for _, file := range testFiles {
		f, err := os.Create(filepath.Join(tempDir, file))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
	}
	for _, dir := range testDirs {
		err := os.Mkdir(filepath.Join(tempDir, dir), 0755)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Call ListDir on the temporary directory
	directoryFiles, err := ListDir(tempDir)
	if err != nil {
		t.Fatalf("ListDir failed: %v", err)
	}
	// Normalize expected and actual file paths to use forward slashes
	var expectedFilesNormalized []string
	for _, file := range directoryFiles {
		expectedFilesNormalized = append(expectedFilesNormalized, filepath.ToSlash(file))
	}
	var actualFilesNormalized []string
	for _, file := range directoryFiles {
		actualFilesNormalized = append(actualFilesNormalized, filepath.ToSlash(file))
	}

	// Verify that the normalized returned list contains all files and directories in the temporary directory
	expectedFiles := append(testFiles, filepath.Join(testDirs[0], "file1.txt"), filepath.Join(testDirs[0], "file2.txt"), filepath.Join(testDirs[0], "file3.txt"), filepath.Join(testDirs[1], "file1.txt"), filepath.Join(testDirs[1], "file2.txt"), filepath.Join(testDirs[1], "file3.txt"))
	if !reflect.DeepEqual(actualFilesNormalized, expectedFilesNormalized) {
		t.Errorf("Expected: %v, got: %v", expectedFiles, actualFilesNormalized)
	}
}
func TestGetTableNameFromPath(t *testing.T) {
	// Test case where file has extension
	fileNameWithExtension := "test/workflow_status.json.gz"
	expectedTableName := "workflow_status"
	actualTableName, err := GetTableNameFromPath(fileNameWithExtension)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if actualTableName != expectedTableName {
		t.Errorf("Expected: %s, got: %s", expectedTableName, actualTableName)
	}

	// Test case where file doesn't have an extension
	fileNameWithoutExtension := "test/workflow_status"
	_, err = GetTableNameFromPath(fileNameWithoutExtension)
	if err == nil || err.Error() != "File does not have an extension" {
		t.Errorf("Expected error: %v, got: %v", errors.New("File does not have an extension"), err)
	}
}

func TestRemoveExtensions(t *testing.T) {
	// Test case where file has extension
	fileNameWithExtension := "test/workflow_status.json.gz"
	expectedFileNameWithoutExtension := "test/workflow_status"
	actualFileNameWithoutExtension, err := removeExtensions(fileNameWithExtension)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if actualFileNameWithoutExtension != expectedFileNameWithoutExtension {
		t.Errorf("Expected: %s, got: %s", expectedFileNameWithoutExtension, actualFileNameWithoutExtension)
	}

	// Test case where file doesn't have an extension
	fileNameWithoutExtension := "test/workflow_status"
	_, err = removeExtensions(fileNameWithoutExtension)
	if err == nil || err.Error() != "File does not have an extension" {
		t.Errorf("Expected error: %v, got: %v", errors.New("File does not have an extension"), err)
	}
}
