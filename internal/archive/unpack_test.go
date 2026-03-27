package archive

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPackUnpackRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := filepath.Join(tmpDir, "original")
	archiveDir := filepath.Join(tmpDir, "archive")
	unpackDir := filepath.Join(tmpDir, "unpacked")

	if err := os.MkdirAll(originalDir, 0o755); err != nil {
		t.Fatalf("Failed to create original dir: %v", err)
	}

	testContent := "This is test content for round-trip verification"
	testFile := filepath.Join(originalDir, "test.txt")
	if err := os.WriteFile(testFile, []byte(testContent), 0o644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	packOpts := PackOptions{
		RootDir:      originalDir,
		OutputDir:    archiveDir,
		ModelName:    "test-model",
		ModelVersion: "1.0",
		Workers:      1,
		ChunkSize:    1024 * 1024,
	}

	ctx := context.Background()
	if err := Pack(ctx, packOpts); err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	unpackOpts := UnpackOptions{
		ParquetFile: filepath.Join(archiveDir, "test-model.parquet"),
		OutputDir:   unpackDir,
		Workers:     1,
	}

	if err := Unpack(ctx, unpackOpts); err != nil {
		t.Fatalf("Unpack failed: %v", err)
	}

	unpackedFile := filepath.Join(unpackDir, "test.txt")
	data, err := os.ReadFile(unpackedFile)
	if err != nil {
		t.Fatalf("Failed to read unpacked file: %v", err)
	}

	if string(data) != testContent {
		t.Fatalf("Unpacked content mismatch: expected %q, got %q", testContent, string(data))
	}
}

func TestPackUnpackMultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := filepath.Join(tmpDir, "original")
	archiveDir := filepath.Join(tmpDir, "archive")
	unpackDir := filepath.Join(tmpDir, "unpacked")

	if err := os.MkdirAll(originalDir, 0o755); err != nil {
		t.Fatalf("Failed to create original dir: %v", err)
	}

	subDir := filepath.Join(originalDir, "subdir")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	files := map[string]string{
		"file1.txt":         "Content 1",
		"file2.txt":         "Content 2",
		"subdir/nested.txt": "Nested content",
	}

	for path, content := range files {
		fullPath := filepath.Join(originalDir, path)
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatalf("Failed to create test file %s: %v", path, err)
		}
	}

	packOpts := PackOptions{
		RootDir:      originalDir,
		OutputDir:    archiveDir,
		ModelName:    "test-model",
		ModelVersion: "1.0",
		Workers:      2,
		ChunkSize:    1024 * 1024,
	}

	ctx := context.Background()
	if err := Pack(ctx, packOpts); err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	unpackOpts := UnpackOptions{
		ParquetFile: filepath.Join(archiveDir, "test-model.parquet"),
		OutputDir:   unpackDir,
		Workers:     2,
	}

	if err := Unpack(ctx, unpackOpts); err != nil {
		t.Fatalf("Unpack failed: %v", err)
	}

	for path, expectedContent := range files {
		unpackedPath := filepath.Join(unpackDir, path)
		data, err := os.ReadFile(unpackedPath)
		if err != nil {
			t.Fatalf("Failed to read unpacked file %s: %v", path, err)
		}

		if string(data) != expectedContent {
			t.Fatalf("File %s content mismatch: expected %q, got %q", path, expectedContent, string(data))
		}
	}
}

func TestPackUnpackBinaryFile(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir := filepath.Join(tmpDir, "original")
	archiveDir := filepath.Join(tmpDir, "archive")
	unpackDir := filepath.Join(tmpDir, "unpacked")

	if err := os.MkdirAll(originalDir, 0o755); err != nil {
		t.Fatalf("Failed to create original dir: %v", err)
	}

	binaryData := []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC}
	binaryFile := filepath.Join(originalDir, "test.bin")
	if err := os.WriteFile(binaryFile, binaryData, 0o644); err != nil {
		t.Fatalf("Failed to create binary test file: %v", err)
	}

	packOpts := PackOptions{
		RootDir:      originalDir,
		OutputDir:    archiveDir,
		ModelName:    "test-model",
		Workers:      1,
		ChunkSize:    1024,
	}

	ctx := context.Background()
	if err := Pack(ctx, packOpts); err != nil {
		t.Fatalf("Pack failed: %v", err)
	}

	unpackOpts := UnpackOptions{
		ParquetFile: filepath.Join(archiveDir, "test-model.parquet"),
		OutputDir:   unpackDir,
		Workers:     1,
	}

	if err := Unpack(ctx, unpackOpts); err != nil {
		t.Fatalf("Unpack failed: %v", err)
	}

	unpackedFile := filepath.Join(unpackDir, "test.bin")
	data, err := os.ReadFile(unpackedFile)
	if err != nil {
		t.Fatalf("Failed to read unpacked binary file: %v", err)
	}

	if len(data) != len(binaryData) {
		t.Fatalf("Binary file size mismatch: expected %d, got %d", len(binaryData), len(data))
	}

	for i, b := range binaryData {
		if data[i] != b {
			t.Fatalf("Binary content mismatch at byte %d: expected 0x%02x, got 0x%02x", i, b, data[i])
		}
	}
}

func TestPackUnpackWithWorkers(t *testing.T) {
	tests := []struct {
		name    string
		workers int
	}{
		{"single worker", 1},
		{"two workers", 2},
		{"four workers", 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			originalDir := filepath.Join(tmpDir, "original")
			archiveDir := filepath.Join(tmpDir, "archive")
			unpackDir := filepath.Join(tmpDir, "unpacked")

			if err := os.MkdirAll(originalDir, 0o755); err != nil {
				t.Fatalf("Failed to create original dir: %v", err)
			}

			testFile := filepath.Join(originalDir, "test.txt")
			if err := os.WriteFile(testFile, []byte("test content"), 0o644); err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			packOpts := PackOptions{
				RootDir:      originalDir,
				OutputDir:    archiveDir,
				ModelName:    "test-model",
				Workers:      tt.workers,
				ChunkSize:    1024 * 1024,
			}

			ctx := context.Background()
			if err := Pack(ctx, packOpts); err != nil {
				t.Fatalf("Pack failed: %v", err)
			}

			unpackOpts := UnpackOptions{
			ParquetFile: filepath.Join(archiveDir, "test-model.parquet"),
			OutputDir:   unpackDir,
			Workers:     tt.workers,
		}
			if err := Unpack(ctx, unpackOpts); err != nil {
				t.Fatalf("Unpack failed: %v", err)
			}

			unpackedFile := filepath.Join(unpackDir, "test.txt")
			if _, err := os.Stat(unpackedFile); err != nil {
				t.Fatalf("Unpacked file not found: %v", err)
			}
		})
	}
}

func TestUnpackNonexistentArchive(t *testing.T) {
	tmpDir := t.TempDir()
	nonexistentArchive := filepath.Join(tmpDir, "nonexistent")
	unpackDir := filepath.Join(tmpDir, "unpacked")

	opts := UnpackOptions{
		ParquetFile: filepath.Join(nonexistentArchive, "test-model.parquet"),
		OutputDir:   unpackDir,
		Workers:     1,
	}

	ctx := context.Background()
	err := Unpack(ctx, opts)
	if err == nil {
		t.Fatal("Unpack should fail for nonexistent archive")
	}
}
