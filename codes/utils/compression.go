package utils

import (
	"io"

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
