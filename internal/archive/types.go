package archive

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
)

type FileRecord struct {
	// File metadata (repeated per chunk)
	FileID        string `parquet:"file_id"`
	FileName      string `parquet:"file_name"`
	Path          string `parquet:"path"`
	FileHash      string `parquet:"file_hash"`
	DateConverted string `parquet:"date_converted"`
	ModelName     string `parquet:"model_name"`
	ModelVersion  string `parquet:"model_version"`
	ContentType   string `parquet:"content_type"` // "text" or "binary"
	SizeBytes     int64  `parquet:"size_bytes"`
	ChunkCount    int64  `parquet:"chunk_count"`
	// Chunk data - stored in typed columns for better compression
	ChunkIndex    int64  `parquet:"chunk_index"`
	TextData      string `parquet:"text_data"`   // UTF-8 text (populated for text files)
	BinaryData    []byte `parquet:"binary_data"` // Raw bytes (populated for binary files)
}

type FileChunk struct {
	FileID     string `parquet:"file_id"`
	ChunkIndex int64  `parquet:"chunk_index"`
	Data       []byte `parquet:"data"`
}

type PackOptions struct {
	RootDir      string
	OutputDir    string
	ModelName    string
	ModelVersion string
	Workers      int
	ChunkSize    int
}

type UnpackOptions struct {
	ParquetFile string
	OutputDir   string
	Workers     int
}

type workItem struct {
	FullPath string
	RelPath  string
	Size     int64
}

func newFileID(relPath string) string {
	sum := sha256.Sum256([]byte(filepath.ToSlash(relPath)))
	return hex.EncodeToString(sum[:])
}

func cleanRelDir(relPath string) string {
	dir := filepath.Dir(relPath)
	if dir == "." {
		return ""
	}
	return filepath.ToSlash(dir)
}

func fileNameFromRelPath(relPath string) string {
	return filepath.Base(relPath)
}

func buildTargetPath(root, relDir, fileName string) string {
	if strings.TrimSpace(relDir) == "" {
		return filepath.Join(root, fileName)
	}
	return filepath.Join(root, filepath.FromSlash(relDir), fileName)
}
