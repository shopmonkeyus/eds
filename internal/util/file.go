package util

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// ListDir will return an array of files recursively walking into sub directories
func ListDir(dir string) (directoryFiles []string, err error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for _, file := range files {
		if file.IsDir() {
			newres, err := ListDir(filepath.Join(dir, file.Name()))
			if err != nil {
				return nil, err
			}
			res = append(res, newres...)
		} else {
			res = append(res, filepath.Join(dir, file.Name()))
		}
	}
	return res, nil
}

func GetTableNameFromPath(fileName string) (string, error) {
	fileNameWithoutExtension, err := removeExtensions(fileName)
	if err != nil {
		return "", err
	}

	return filepath.Base(fileNameWithoutExtension), nil
}

func removeExtensions(fileName string) (string, error) {
	if filepath.Ext(fileName) == "" {
		err := errors.New("File does not have an extension")
		return "", err
	}
	for {
		ext := filepath.Ext(fileName)
		if ext == "" {
			break
		}
		fileName = strings.TrimSuffix(fileName, ext)
	}
	return fileName, nil
}
