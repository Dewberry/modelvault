package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	pqgzip "github.com/parquet-go/parquet-go/compress/gzip"
)

func Pack(ctx context.Context, opts PackOptions) error {
	rootDir, err := filepath.Abs(opts.RootDir)
	if err != nil {
		return fmt.Errorf("resolve root dir: %w", err)
	}
	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 16 * 1024 * 1024 // 16MB chunks to reduce metadata overhead
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	filename := fmt.Sprintf("%s.parquet", opts.ModelName)
	recordPath := filepath.Join(opts.OutputDir, filename)

	workCh := make(chan workItem, opts.Workers*2)
	recordCh := make(chan FileRecord, opts.Workers*2)
	errCh := make(chan error, 1)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		if err := writeRecords(recordPath, recordCh); err != nil {
			sendErr(errCh, err)
			cancel()
		}
	}()

	var workerWG sync.WaitGroup
	for i := 0; i < opts.Workers; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for item := range workCh {
				if err := processFile(ctx, item, opts, recordCh); err != nil {
					sendErr(errCh, err)
					cancel()
					return
				}
			}
		}()
	}

	walkErr := filepath.WalkDir(rootDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		rel, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}

		// Skip excluded directories
		if d.IsDir() && shouldExcludeDir(rel, opts.ExcludeSubdirs) {
			return filepath.SkipDir
		}

		if d.IsDir() {
			return nil
		}

		// Skip excluded file extensions
		if shouldExcludeFile(path, opts.ExcludeExtensions) {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		select {
		case workCh <- workItem{FullPath: path, RelPath: rel, Size: info.Size()}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	close(workCh)
	workerWG.Wait()
	close(recordCh)
	writerWG.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	if walkErr != nil && walkErr != context.Canceled {
		return fmt.Errorf("walk directory: %w", walkErr)
	}
	return nil
}

func processFile(ctx context.Context, item workItem, opts PackOptions, recordCh chan<- FileRecord) error {
	f, err := os.Open(item.FullPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", item.FullPath, err)
	}
	defer f.Close()

	h := sha256.New()
	buf := make([]byte, opts.ChunkSize)
	fileID := newFileID(item.RelPath)
	var (
		totalBytes int64
		sample     []byte
	)

	// First pass: detect content type, calculate hash
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, readErr := f.Read(buf)
		if n > 0 {
			piece := make([]byte, n)
			copy(piece, buf[:n])

			if _, err := h.Write(piece); err != nil {
				return fmt.Errorf("hash %s: %w", item.FullPath, err)
			}
			if len(sample) < 8192 {
				need := 8192 - len(sample)
				if need > n {
					need = n
				}
				sample = append(sample, piece[:need]...)
			}

			totalBytes += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read %s: %w", item.FullPath, readErr)
		}
	}

	contentType := "binary"
	if isProbablyText(sample) {
		contentType = "text"
	}

	dateConverted := time.Now().UTC().Format(time.RFC3339)
	fileHash := hex.EncodeToString(h.Sum(nil))

	// Re-read file to write chunks with metadata
	f.Seek(0, 0)
	buf = make([]byte, opts.ChunkSize)
	var chunkIndex int64 = 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, readErr := f.Read(buf)
		if n > 0 {
			piece := make([]byte, n)
			copy(piece, buf[:n])

			// Store data in typed column: text as string, binary as raw bytes (pre-compressed)
			var textData string
			var binaryData []byte
			if contentType == "text" {
				textData = string(piece)
			} else {
				// Pre-compress binary chunks for better compression
				var buf bytes.Buffer
				gw := gzip.NewWriter(&buf)
				if _, err := gw.Write(piece); err != nil {
					return fmt.Errorf("compress chunk: %w", err)
				}
				if err := gw.Close(); err != nil {
					return fmt.Errorf("close gzip: %w", err)
				}
				binaryData = buf.Bytes()
			}

			record := FileRecord{
				FileID:        fileID,
				FileName:      fileNameFromRelPath(item.RelPath),
				Path:          cleanRelDir(item.RelPath),
				FileHash:      fileHash,
				DateConverted: dateConverted,
				ModelName:     opts.ModelName,
				ModelVersion:  opts.ModelVersion,
				ContentType:   contentType,
				SizeBytes:     totalBytes,
				ChunkIndex:    chunkIndex,
				TextData:      textData,
				BinaryData:    binaryData,
			}

			select {
			case recordCh <- record:
			case <-ctx.Done():
				return ctx.Err()
			}

			chunkIndex++
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read %s: %w", item.FullPath, readErr)
		}
	}

	return nil
}

func writeRecords(path string, records <-chan FileRecord) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create parquet file: %w", err)
	}
	defer f.Close()

	var recordCount int
	var writer *parquet.Writer

	for record := range records {
		// Lazily create writer on first record with GZIP compression
		// MaxRowsPerRowGroup ensures all chunks from a file are in same row group for better compression
		if writer == nil {
			writer = parquet.NewWriter(f,
				parquet.Compression(&pqgzip.Codec{}),
				parquet.MaxRowsPerRowGroup(10000),     // Large enough for most files
				parquet.WriteBufferSize(16*1024*1024), // 16MB buffer for better compression
			)
		}
		if err := writer.Write(&record); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
		recordCount++
	}

	// Only close writer if we created it (wrote records)
	if writer != nil {
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close parquet writer: %w", err)
		}
	} else {
		// No records written - return error since we can't create empty parquet
		return fmt.Errorf("no files found to archive")
	}

	return nil
}

func sendErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func shouldExcludeFile(path string, excludeExtensions []string) bool {
	if len(excludeExtensions) == 0 {
		return false
	}
	ext := filepath.Ext(path)
	for _, exc := range excludeExtensions {
		if ext == exc || "."+ext == exc {
			return true
		}
	}
	return false
}

func shouldExcludeDir(relPath string, excludeSubdirs []string) bool {
	if len(excludeSubdirs) == 0 {
		return false
	}
	parts := filepath.SplitList(filepath.ToSlash(relPath))
	for _, part := range parts {
		for _, exc := range excludeSubdirs {
			if part == exc {
				return true
			}
		}
	}
	return false
}
