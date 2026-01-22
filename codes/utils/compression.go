package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

var globalEnc *zstd.Encoder
var globalDec *zstd.Decoder

func init() {
	var err error
	globalEnc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		panic(err)
	}
	globalDec, err = zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
}

func Compress(in io.Reader, out io.Writer) error {
	globalEnc.Reset(out)

	var err error
	_, err = io.Copy(globalEnc, in)
	if err != nil {
		globalEnc.Close()
		return err
	}
	return globalEnc.Close()
}

func CompressWith(in io.Reader, out io.Writer, enc *zstd.Encoder) error {
	enc.Reset(out)

	var err error
	_, err = io.Copy(globalEnc, in)
	if err != nil {
		globalEnc.Close()
		return err
	}
	return globalEnc.Close()
}

func Decompress(in io.Reader, out io.Writer) error {
	globalDec.Reset(in)

	var err error
	_, err = io.Copy(out, globalDec)
	if err != nil {
		globalDec.Close()
		return err
	}
	return nil
}

func DecompressWith(in io.Reader, out io.Writer, dec *zstd.Decoder) error {
	dec.Reset(in)

	var err error
	_, err = io.Copy(out, dec)
	if err != nil {
		dec.Close()
		return err
	}
	return nil
}

// DecompressFile streams a .zst file into a plain output file without loading it all in memory.
func DecompressFile(srcPath, dstPath string) error {
	in, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source failed: %w", err)
	}
	defer in.Close()

	out, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create destination failed: %w", err)
	}

	if err := Decompress(in, out); err != nil {
		out.Close()
		return fmt.Errorf("decompress failed: %w", err)
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return fmt.Errorf("sync destination failed: %w", err)
	}
	return out.Close()
}

// [CRITICAL] not able to handle csv lines with embedded newlines
func ProcessCSVZstdLinesFromFile(srcPath string, handle func(line []byte) error) error {
	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer in.Close()

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		if err := Decompress(in, pw); err != nil {
			_ = pw.CloseWithError(err)
		}
	}()

	sc := bufio.NewScanner(pr)

	sc.Buffer(make([]byte, 1<<20), 1<<26) // ~64MB
	for sc.Scan() {
		if err := handle(sc.Bytes()); err != nil {
			return err
		}
	}
	return sc.Err()
}
