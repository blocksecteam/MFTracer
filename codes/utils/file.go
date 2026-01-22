package utils

import (
	"bufio"
	"encoding/csv"
	"os"
	"transfer-graph-evm/model"
)

func ReadAddressFile(path string) ([]model.Address, error) {
	addressFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	addrs := make([]model.Address, 0)
	scanner := bufio.NewScanner(addressFile)
	for scanner.Scan() {
		if scanner.Text() == "" {
			continue
		}
		addrs = append(addrs, model.HexToAddress(scanner.Text()))
	}
	return addrs, nil
}

func ReadAddressFileFmtCSV(path string, filter func([]string) (model.Address, bool)) ([]model.Address, error) {
	addressFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer addressFile.Close()
	reader := csv.NewReader(addressFile)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	addrs := make([]model.Address, 0, len(records))
	for _, record := range records {
		if addr, ok := filter(record); ok {
			addrs = append(addrs, addr)
		}
	}
	return addrs, nil
}

func WriteAddressFile(path string, addrs []model.Address) error {
	addressFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer addressFile.Close()
	writer := csv.NewWriter(addressFile)
	for _, addr := range addrs {
		if err := writer.Write([]string{addr.String()}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}
