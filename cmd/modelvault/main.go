package main

import (
	"context"
	"flag"
	"fmt"
	"os"
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
  modelvault pack --dir <directory> --out <output-dir> --model <model-name> [--model-version <version>] [--workers N] [--chunk-size-bytes N]
  modelvault unpack --in <parquet-file> --outdir <directory> [--workers N]

Commands:
  pack    Recursively scan a directory and write unified parquet (<model-name>.parquet)
  unpack  Reconstruct files from a parquet file
`)
}

func runPack(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("pack", flag.ContinueOnError)

	dir := fs.String("dir", "", "directory to scan recursively")
	out := fs.String("out", "archive_out", "output dataset directory")
	model := fs.String("model", "", "model name")
	modelVersion := fs.String("model-version", "", "optional model version")
	workers := fs.Int("workers", runtime.NumCPU(), "number of concurrent file workers")
	chunkSize := fs.Int("chunk-size-bytes", 1024*1024, "chunk size in bytes")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*dir) == "" {
		return fmt.Errorf("--dir is required")
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
		RootDir:      *dir,
		OutputDir:    *out,
		ModelName:    *model,
		ModelVersion: *modelVersion,
		Workers:      *workers,
		ChunkSize:    *chunkSize,
	}
	return archive.Pack(ctx, opts)
}

func runUnpack(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("unpack", flag.ContinueOnError)

	in := fs.String("in", "", "input parquet file")
	outdir := fs.String("outdir", "", "directory to reconstruct files into")
	workers := fs.Int("workers", runtime.NumCPU(), "number of concurrent file writers")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*in) == "" {
		return fmt.Errorf("--in is required")
	}
	if strings.TrimSpace(*outdir) == "" {
		return fmt.Errorf("--outdir is required")
	}
	if *workers <= 0 {
		return fmt.Errorf("--workers must be > 0")
	}

	opts := archive.UnpackOptions{
		ParquetFile: *in,
		OutputDir:   *outdir,
		Workers:     *workers,
	}
	return archive.Unpack(ctx, opts)
}
