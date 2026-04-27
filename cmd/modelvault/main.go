package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"modelvault/internal/archive"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	ctx := context.Background()

	switch os.Args[1] {
	case "pack":
		if err := runPack(ctx, os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "pack failed: %v\n", err)
			os.Exit(1)
		}
	case "unpack":
		if err := runUnpack(ctx, os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "unpack failed: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`Usage:
  modelvault pack --input-dir <directory> --model <model-name> [--output-dir <output-dir>] [--model-version <version>] [--workers N] [--chunk-size-bytes N]
  modelvault unpack --input-file <parquet-file> --output-dir <directory> [--workers N]

Commands:
  pack    Recursively scan a directory and write unified parquet (<model-name>.parquet)
  unpack  Reconstruct files from a parquet file
`)
}

func runPack(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pack", flag.ContinueOnError)

	inputDir := fs.String("input-dir", "", "directory to scan recursively")
	outputDir := fs.String("output-dir", "vault", "output dataset directory")
	model := fs.String("model", "", "model name")
	modelVersion := fs.String("model-version", "", "optional model version")
	workers := fs.Int("workers", runtime.NumCPU(), "number of concurrent file workers")
	chunkSize := fs.Int("chunk-size-bytes", 100*1024*1024, "chunk size in bytes")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*inputDir) == "" {
		return fmt.Errorf("--input-dir is required")
	}
	if strings.TrimSpace(*model) == "" {
		return fmt.Errorf("--model is required")
	}
	if *workers <= 0 {
		return fmt.Errorf("--workers must be > 0")
	}
	if *chunkSize <= 0 {
		return fmt.Errorf("--chunk-size-bytes must be > 0")
	}

	opts := archive.PackOptions{
		RootDir:      *inputDir,
		OutputDir:    *outputDir,
		ModelName:    *model,
		ModelVersion: *modelVersion,
		Workers:      *workers,
		ChunkSize:    *chunkSize,
	}
	if err := archive.Pack(ctx, opts); err != nil {
		return fmt.Errorf("pack failed: %w", err)
	}
	outputFile := filepath.Join(*outputDir, fmt.Sprintf("%s.parquet", *model))
	fmt.Printf("Successfully packed model to: %s\n", outputFile)
	return nil
}

func runUnpack(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("unpack", flag.ContinueOnError)

	inputFile := fs.String("input-file", "", "input parquet file")
	outputDir := fs.String("output-dir", "", "directory to reconstruct files into")
	workers := fs.Int("workers", runtime.NumCPU(), "number of concurrent file writers")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*inputFile) == "" {
		return fmt.Errorf("--input-file is required")
	}
	if strings.TrimSpace(*outputDir) == "" {
		return fmt.Errorf("--output-dir is required")
	}
	if *workers <= 0 {
		return fmt.Errorf("--workers must be > 0")
	}

	opts := archive.UnpackOptions{
		ParquetFile: *inputFile,
		OutputDir:   *outputDir,
		Workers:     *workers,
	}
	if err := archive.Unpack(ctx, opts); err != nil {
		return fmt.Errorf("unpack failed: %w", err)
	}
	fmt.Printf("Successfully unpacked files to: %s\n", *outputDir)
	return nil
}
