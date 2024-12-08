package utils

import (
	"path/filepath"
	"runtime"
)

func GetPath(relativePath string) string {
	// Get the current file's directory
	_, currentFile, _, ok := runtime.Caller(1)
	if !ok {
		panic("Unable to resolve current file path")
	}
	// Resolve the absolute path
	basePath := filepath.Dir(currentFile)
	return filepath.Join(basePath, relativePath)
}