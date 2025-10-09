package pricedb_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"transfer-graph/pricedb"

	"github.com/ethereum/go-ethereum/common"
)

const pDBPath = "/data/price-db"
const tokensDir = "/home/icecy/transfer-graph/material/decimals"
const tokensFile = "decimals.csv"

func TestSync(t *testing.T) {
	/*
		config := &opensearch.OpenSearchConfig{
			Index:    "eth_block",
			Url:      "http://192.168.3.146:9200",
			User:     "yicheng_huo",
			Password: "yicheng_huo@blocksecHYC123hyc",
		}
	*/
	p, err := pricedb.NewPriceDB(pDBPath, false)
	if err != nil {
		t.Error(err)
		return
	}
	defer p.Close()
	tokens, err := pricedb.ExtractTokenList(tokensDir, tokensFile)
	if err != nil {
		t.Error(err)
		return
	}
	//tokens = tokens[:200]
	/*
		err = pricedb.SyncByOpenSearch(p, 16500000, 16510000, tokens, context.Background(), config)
		if err != nil {
			t.Error(err)
			return
		}
		err = pricedb.SimpleSyncDecimals(p, tokensDir, tokensFile)
		if err != nil {
			t.Error(err)
			return
		}
	*/

	blocks := make([]uint64, len(tokens))
	for i := range blocks {
		blocks[i] = 16400000
	}
	prices, err := p.TokensWithBlocks(tokens, blocks, 2, context.Background())
	if err != nil {
		t.Error(err)
		return
	}
	retString := ""
	for i, token := range tokens {
		retString += token.Hex() + fmt.Sprintf("\t\t%f\n", prices[i])
	}
	file, err := os.Create("temp.txt")
	if err != nil {
		t.Error(err)
		return
	}
	defer file.Close()
	_, err = file.WriteString(retString)
	if err != nil {
		t.Error(err)
		return
	}
	decimalss, err := p.SimpleReadAllDecimals()
	if err != nil {
		t.Error(err)
		return
	}
	retString = ""
	for token, decimals := range decimalss {
		retString += common.BytesToAddress([]byte(token)).Hex() + fmt.Sprintf(",%d\n", decimals)
	}
	_, err = file.WriteString(retString)
	if err != nil {
		t.Error(err)
		return
	}
}
