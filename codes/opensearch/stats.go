package opensearch

import (
	"bytes"
	"fmt"
	"os"
	"path"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/btree"
)

type Statistics struct {
	OutputEnabled bool
	OutputPath    string

	uniqueAddress *btree.BTreeG[common.Address]
	uniqueToken   *btree.BTreeG[common.Address]
	uniqueNft     *btree.BTreeG[common.Address]
	uniqueErc1155 *btree.BTreeG[common.Address]
	nft           map[common.Address]struct{}
	disabled      bool
}

func NewStatistics() *Statistics {
	less := func(a, b common.Address) bool { return bytes.Compare(a[:], b[:]) == -1 }
	degree := 8
	return &Statistics{
		uniqueAddress: btree.NewG[common.Address](degree, less),
		uniqueToken:   btree.NewG[common.Address](degree, less),
		uniqueNft:     btree.NewG[common.Address](degree, less),
		uniqueErc1155: btree.NewG[common.Address](degree, less),
		nft:           make(map[common.Address]struct{}),
		disabled:      true,

		OutputEnabled: false,
		OutputPath:    "",
	}
}

func (s *Statistics) LoadNFTs(f string) error {
	raw, err := os.ReadFile(f)
	if err != nil {
		return fmt.Errorf("load nft failed: %s", err.Error())
	}
	result := make([]common.Address, 0)
	if err := json.Unmarshal(raw, &result); err != nil {
		return fmt.Errorf("parse nft failed: %s", err.Error())
	}
	for _, a := range result {
		s.nft[a] = struct{}{}
	}
	log.Info("nft address loaded", "count", len(s.nft))
	return nil
}

func (s *Statistics) Enable()  { s.disabled = false }
func (s *Statistics) Disable() { s.disabled = true }

func (s *Statistics) Finish() error {
	if s.disabled {
		return nil
	}
	if !s.OutputEnabled {
		return nil
	}
	outputPath := s.OutputPath
	if _, err := os.Stat(s.OutputPath); os.IsNotExist(err) {
		return err
	}
	write := func(filename string, entry *btree.BTreeG[common.Address]) error {
		f, err := os.Create(filename)
		if err != nil {
			return err
		}
		entry.Ascend(func(item common.Address) bool {
			_, err := f.WriteString(item.String() + "\n")
			if err != nil {
				log.Warn("write failed", "f", f.Name(), "err", err.Error())
			}
			return err == nil
		})
		if err := f.Close(); err != nil {
			return err
		}
		return nil
	}

	if err := write(path.Join(outputPath, "addr.json"), s.uniqueAddress); err != nil {
		log.Info("finish addr failed", "err", err.Error())
		return err
	}
	if err := write(path.Join(outputPath, "token.json"), s.uniqueToken); err != nil {
		log.Info("finish token failed", "err", err.Error())
		return err
	}
	if err := write(path.Join(outputPath, "nft.json"), s.uniqueNft); err != nil {
		log.Info("finish nft failed", "err", err.Error())
		return err
	}
	if err := write(path.Join(outputPath, "erc1155.json"), s.uniqueErc1155); err != nil {
		log.Info("finish erc1155 failed", "err", err.Error())
		return err
	}
	return nil
}
