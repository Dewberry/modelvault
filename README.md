# modelvault
Archive multi-file environmental models to Parquet with SQL-queryable results and memory-efficient streaming locally or from the cloud.

## Installation

Build the binary from source:

```bash
make build
# or
go build -o modelvault ./cmd/modelvault
```

## Usage

modelvault provides two main commands: `pack` and `unpack`.

### Pack Command

Convert a directory of model files into a single Parquet archive with metadata.

```
modelvault pack --input-dir <directory> --output-dir <output-dir> --model <model-name> [OPTIONS]
```

**Options:**
- `--input-dir` (required): Directory to recursively scan for files
- `--output-dir` (optional): Output directory for the Parquet file (default: `vault`)
- `--model` (required): Model name (used for the output filename)
- `--model-version`: Optional model version (stored in metadata)
- `--workers`: Number of concurrent file workers (default: number of CPU cores)
- `--chunk-size-bytes`: Chunk size for streaming large files (default: 100MB)

**Examples:**

Basic packing:
```bash
modelvault pack --input-dir ./my-watershed-version-1 --model my-watershed
# Creates: ./vault/my-watershed.parquet
```

With version and custom output directory:
```bash
modelvault pack --input-dir ./models/my-watershed-version-2.1 --output-dir ./archive \
  --model my-watershed --model-version 2.1
```

Custom chunk size and workers:
```bash
modelvault pack --input-dir ./large_dataset --output-dir ./output \
  --model dataset --chunk-size-bytes 52428800 --workers 8  # 50MB chunks
```

### Unpack Command

Reconstruct files from a Parquet archive back to their original directory structure.

```
modelvault unpack --input-file <parquet-file> --output-dir <directory> [OPTIONS]
```

**Options:**
- `--input-file` (required): Path to the Parquet file
- `--output-dir` (required): Directory to reconstruct files into
- `--workers`: Number of concurrent file writers (default: number of CPU cores)

**Examples:**

Basic unpacking:
```bash
modelvault unpack --input-file ./vault/my_model_v1.parquet --output-dir ./restored
```

With custom workers for faster extraction:
```bash
modelvault unpack --input-file ./archive/model.parquet --output-dir ./data --workers 16
```

## Use Cases

- **Model Distribution**: Package complex environmental models with multiple files into a single, portable Parquet file
- **Data Archival**: Compress and store model files with full metadata for SQL querying
- **Cloud Storage**: Efficiently store and stream model files from cloud storage with metadata
- **Reproducibility**: Maintain complete model versions with consistent chunking and hashing

## Output Structure

The generated Parquet file contains a table with the following columns:

| Column | Description |
|--------|-------------|
| `file_id` | Unique identifier for each file |
| `file_name` | Name of the file |
| `path` | Relative path within the original directory |
| `file_hash` | SHA256 hash of the file content |
| `date_converted` | Timestamp when the file was packed |
| `model_name` | Model name provided during packing |
| `model_version` | Optional version string |
| `content_type` | File type: "text" or "binary" |
| `size_bytes` | Total file size in bytes |
| `chunk_index` | 0-based chunk number for streaming large files |
| `text_data` | UTF-8 text content (for text files) |
| `binary_data` | Raw bytes (for binary files) |

## Features

- **Memory-Efficient Streaming**: Automatically chunks large files for safe processing
- **Recursive Directory Scanning**: Captures complete directory structures
- **Concurrent Processing**: Parallel file processing with configurable worker count
- **Metadata Preservation**: Stores file hashes, sizes, paths, and timestamps
- **Flexible Format**: Supports both text and binary files with automatic detection
