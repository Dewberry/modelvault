package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestPack(t *testing.T) {
	tmpDir := t.TempDir()
	outputDir := filepath.Join(tmpDir, "output")

	testFile1 := filepath.Join(tmpDir, "file1.txt")
	testContent1 := "Hello, World!"
	if err := os.WriteFile(testFile1, []byte(testContent1), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	testDir := filepath.Join(tmpDir, "subdir")
	if err := os.MkdirAll(testDir, 0o755); err != nil {
		t.Fatalf("Failed to create test subdirectory: %v", err)
	}

	testFile2 := filepath.Join(testDir, "file2.txt")
	testContent2 := "Nested file content"
	if err := os.WriteFile(testFile2, []byte(testContent2), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	opts := PackOptions{
		RootDir:      tmpDir,
		OutputDir:    outputDir,
		ModelName:    "test-model",
		ModelVersion: "1.0",
		Workers:      2,
		ChunkSize:    1024 * 1024,
	}

	ctx := context.Background()
	if err := Pack(ctx, opts); err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	archivePath := filepath.Join(outputDir, "test-model.parquet")
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("Archive file not created: %v", err)
	}

	info, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("Failed to stat archive: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("Archive is empty")
	}
}

func TestPackWithBinaryFile(t *testing.T) {
	tmpDir := t.TempDir()
	outputDir := filepath.Join(tmpDir, "output")

	testFile := filepath.Join(tmpDir, "binary.bin")
	binaryData := []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD}
	if err := os.WriteFile(testFile, binaryData, 0o644); err != nil {
		t.Fatalf("Failed to create binary test file: %v", err)
	}

	opts := PackOptions{
		RootDir:      tmpDir,
		OutputDir:    outputDir,
		ModelName:    "test-model",
		ModelVersion: "1.0",
		Workers:      1,
		ChunkSize:    1024,
	}

	ctx := context.Background()
	if err := Pack(ctx, opts); err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	archivePath := filepath.Join(outputDir, "test-model.parquet")
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("Archive file not created: %v", err)
	}
}

func TestPackLargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	outputDir := filepath.Join(tmpDir, "output")

	testFile := filepath.Join(tmpDir, "large.dat")
	largeData := make([]byte, 5*1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, largeData, 0o644); err != nil {
		t.Fatalf("Failed to create large test file: %v", err)
	}

	opts := PackOptions{
		RootDir:      tmpDir,
		OutputDir:    outputDir,
		ModelName:    "test-model",
		ModelVersion: "1.0",
		Workers:      2,
		ChunkSize:    512 * 1024,
	}

	ctx := context.Background()
	if err := Pack(ctx, opts); err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	archivePath := filepath.Join(outputDir, "test-model.parquet")
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("Archive file not created: %v", err)
	}
}

func TestComputeHash(t *testing.T) {
	data := []byte("test data")
	h := sha256.New()
	h.Write(data)
	hash := hex.EncodeToString(h.Sum(nil))

	if hash == "" {
		t.Fatal("Hash should not be empty")
	}
	if len(hash) != 64 {
		t.Fatalf("SHA256 hash should be 64 hex characters, got %d", len(hash))
	}
}
