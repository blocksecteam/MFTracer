package opensearch

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"time"
	"transfer-graph/model"

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

func (s *Statistics) Dump(r *model.QueryResult) {
	if s.disabled {
		return
	}
	startTime := time.Now()

	externalCount := 0
	internalCount := 0
	erc20Count, nftCount := 0, 0
	erc1155Single, erc1155Batch := 0, 0
	mints, burns := 0, 0
	uniqueAddress := make(map[common.Address]struct{})
	uniqueToken := make(map[common.Address]struct{})
	uniqueNft := make(map[common.Address]struct{})
	uniqueErc1155 := make(map[common.Address]struct{})
	zeroAddress := common.Address{}
	wethDeposit, wethWithdraw := 0, 0
	for _, t := range r.Transfers {
		uniqueAddress[t.From] = struct{}{}
		uniqueAddress[t.To] = struct{}{}
		uniqueToken[t.Token] = struct{}{}

		switch t.Type {
		case uint16(model.TransferTypeExternal):
			externalCount += 1
		case uint16(model.TransferTypeInternal):
			internalCount += 1
		case uint16(model.TransferTypeEvent):
			a := t.Token
			if _, ok := s.nft[a]; ok {
				uniqueNft[t.Token] = struct{}{}
				nftCount += 1
			} else {
				erc20Count += 1
			}
			if t.From == zeroAddress {
				mints += 1
			}
			if t.To == zeroAddress {
				burns += 1
			}
		case uint16(model.TransferTypeWETHDeposit):
			wethDeposit += 1
		case uint16(model.TransferTypeWETHWithdraw):
			wethWithdraw += 1
		case uint16(model.TransferTypeERC1155Single):
			uniqueErc1155[t.Token] = struct{}{}
			erc1155Single += 1
		case uint16(model.TransferTypeERC1155Batch):
			uniqueErc1155[t.Token] = struct{}{}
			erc1155Batch += 1
		}
	}
	for a := range uniqueAddress {
		s.uniqueAddress.ReplaceOrInsert(a)
	}
	for a := range uniqueToken {
		s.uniqueToken.ReplaceOrInsert(a)
	}
	for a := range uniqueNft {
		s.uniqueNft.ReplaceOrInsert(a)
	}
	for a := range uniqueErc1155 {
		s.uniqueErc1155.ReplaceOrInsert(a)
	}

	values := make([]interface{}, 0)
	values = append(values, "duration", time.Since(startTime))
	values = append(values, "transferCount", len(r.Transfers))
	values = append(values, "txCount", len(r.Txs))
	values = append(values, "externalCount", externalCount)
	values = append(values, "internalCount", internalCount)
	values = append(values, "erc20Count", erc20Count)
	values = append(values, "nftCount", nftCount)
	values = append(values, "mintCount", mints)
	values = append(values, "burnCount", burns)
	values = append(values, "wethDepositCount", wethDeposit)
	values = append(values, "wethWithdrawCount", wethWithdraw)
	values = append(values, "erc1155Single", erc1155Single)
	values = append(values, "erc1155Batch", erc1155Batch)
	values = append(values, "uniqueAddress", len(uniqueAddress))
	values = append(values, "uniqueToken", len(uniqueToken))
	values = append(values, "uniqueNft", len(uniqueNft))
	values = append(values, "uniqueErc1155", len(uniqueErc1155))
	values = append(values, "totalUniqueAddress", s.uniqueAddress.Len())
	values = append(values, "totalUniqueToken", s.uniqueToken.Len())
	values = append(values, "totaluniqueNft", s.uniqueNft.Len())
	values = append(values, "totaluniqueErc1155", s.uniqueErc1155.Len())
	log.Info("statistics", values...)
}

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
