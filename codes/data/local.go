package data

import (
	"fmt"
	"os"
	"transfer-graph-evm/model"

	"github.com/klauspost/compress/zstd"
)

func LoadQueryResult(filePath string) (*model.QueryResult, error) {
	p := filePath
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open file failed (file: %s): %s", p, err.Error())
	}
	dec, err := zstd.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("zstd create decoder failed: %s", err.Error())
	}
	s := &model.QueryResult{}
	if err := json.NewDecoder(dec).Decode(s); err != nil {
		return nil, fmt.Errorf("json decode failed: %s", err.Error())
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("close file failed: %s", err.Error())
	}
	return s, nil
}

func DumpQueryResult(p string, s *model.QueryResult) error {
	outFile, err := os.Create(p)
	if err != nil {
		return fmt.Errorf("create file failed: %s", err.Error())
	}
	enc, err := zstd.NewWriter(outFile)
	if err != nil {
		return fmt.Errorf("zstd create encoder failed: %s", err.Error())
	}
	err = json.NewEncoder(enc).Encode(s)
	if err != nil {
		return fmt.Errorf("json encode failed: %s", err.Error())
	}
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close encoder failed: %s", err.Error())
	}
	if err := outFile.Close(); err != nil {
		return fmt.Errorf("close file failed: %s", err.Error())
	}
	return nil
}
