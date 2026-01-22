package pricedb

import (
	"os"
	"path"
	"strings"
	"transfer-graph-evm/model"
)

// extract from file formatted as r"([addr],[decimals]\n)*[addr],[decimals]"
func ExtractTokenList(dataDir, fileName string) ([]model.Address, error) {
	file, err := os.ReadFile(path.Join(dataDir, fileName))
	if err != nil {
		return nil, err
	}
	records := strings.Split(string(file), "\n")
	tokens := make([]model.Address, 0, len(records))
	for _, record := range records {
		items := strings.Split(record, ",")
		if len(items[0]) != 20*2+2 {
			continue
		}
		tokens = append(tokens, model.HexToAddress(items[0]))
	}
	return tokens, nil
}
