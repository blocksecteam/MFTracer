package data

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"transfer-graph-evm/model"
	"transfer-graph-evm/utils"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/fbsobreira/gotron-sdk/pkg/common"
	"golang.org/x/sync/errgroup"
)

const URL_SCHEME string = "http"
const URL_HOST string = "localhost:8002"
const URL_TokenTransaction_PATH string = "/api/v1/token/transaction"
const URL_TokenTransaction string = "http://localhost:8882/api/v1/token/transaction"
const URL_ConditionTokenTransaction_PATH string = "/api/v1/condition/token/transaction"
const URL_ConditionTokenTransaction string = "http://localhost:8882/api/v1/condition/token/transaction"

const GLOBAL_TOTAL_LIMIT int = 2000  // Global limit for the number of transactions to fetch
const GLOBAL_SINGLE_LIMIT int = 1000 // Limit for each single request to the API

type Req_ConditionTokenTransaction struct {
	ChainId int `json:"ChainId"`
	//From           string `json:"From"`
	To             string `json:"To"`
	Coin           string `json:"Coin"`
	StartTimestamp string `json:"StartTimestamp"`
	//EndTimestamp   string `json:"EndTimestamp"`
	Limit  int `json:"Limit"`
	Offset int `json:"Offset"`
}

type Res_Transaction struct {
	From      string  `json:"From"`
	To        string  `json:"To"`
	Type      uint32  `json:"Type"`
	Identity  uint32  `json:"Identity"`
	Coin      string  `json:"Coin"`
	Value     string  `json:"Value"`
	Block     uint64  `json:"Number"`
	Index     uint32  `json:"Index"`
	TxHash    string  `json:"Tx"`
	Timestamp string  `json:"Timestamp"`
	TxFrom    string  `json:"TxFrom"`
	TxTo      string  `json:"TxTo"`
	RealValue float64 `json:"TokenPrice"`
}

type Res_Transaction_Byte struct {
	From      []byte  `json:"from"`
	To        []byte  `json:"to"`
	Type      uint32  `json:"type"`
	Identity  uint32  `json:"identity"`
	Coin      []byte  `json:"coin"`
	Value     []byte  `json:"value"`
	Block     string  `json:"number"`
	Index     uint32  `json:"index"`
	TxHash    []byte  `json:"tx"`
	Timestamp string  `json:"timestamp"`
	TxFrom    []byte  `json:"tx_from"`
	TxTo      []byte  `json:"tx_to"`
	RealValue float64 `json:"token_price"`
}

func (r *Res_Transaction_Byte) Decode() (*model.Transfer, error) {
	ret := &model.Transfer{}
	if block, err := strconv.ParseUint(r.Block, 10, 64); err == nil {
		ret.Pos = block<<16 | uint64(r.Index)
	} else {
		return nil, err
	}
	ret.Txid = uint16(r.Identity)
	ret.Type = uint16(r.Type)
	ret.From = model.BytesToAddress(r.From)
	ret.To = model.BytesToAddress(r.To)
	ret.Token = model.BytesToAddress(r.Coin)
	ret.Value = (*hexutil.Big)(big.NewInt(0).SetBytes(r.Value))
	if unixTimeMs, err := strconv.ParseInt(r.Timestamp, 10, 64); err == nil {
		ret.Timestamp = time.UnixMilli(unixTimeMs).Format(time.RFC3339)
	} else {
		return nil, err
	}
	ret.TxHash = common.BytesToHash(r.TxHash)
	return ret, nil
}

type Res_Transaction_String struct {
	From      string  `json:"from"`
	To        string  `json:"to"`
	Type      uint32  `json:"type"`
	Identity  uint32  `json:"identity"`
	Coin      string  `json:"coin"`
	Value     string  `json:"value"`
	Block     string  `json:"number"`
	Index     uint32  `json:"index"`
	TxHash    string  `json:"tx"`
	Timestamp string  `json:"timestamp"`
	TxFrom    string  `json:"tx_from"`
	TxTo      string  `json:"tx_to"`
	RealValue float64 `json:"token_price"`
}

func (r *Res_Transaction_String) Decode() (*model.Transfer, error) {
	ret := &model.Transfer{}
	if block, err := strconv.ParseUint(r.Block, 10, 64); err == nil {
		ret.Pos = block<<16 | uint64(r.Index)
	} else {
		return nil, err
	}
	ret.Txid = uint16(r.Identity)
	ret.Type = uint16(r.Type)
	ret.From = model.HexToAddress(r.From)
	ret.To = model.HexToAddress(r.To)
	ret.Token = model.HexToAddress(r.Coin)
	if value, success := big.NewInt(0).SetString(r.Value, 16); success {
		ret.Value = (*hexutil.Big)(value)
	} else {
		return nil, fmt.Errorf("invalid value: %s", r.Value)
	}
	if unixTimeMs, err := strconv.ParseInt(r.Timestamp, 10, 64); err == nil {
		ret.Timestamp = time.UnixMilli(unixTimeMs).Format(time.RFC3339)
	} else {
		return nil, err
	}
	if hash, err := common.HexToHash(r.TxHash); err == nil {
		ret.TxHash = hash
	} else {
		return nil, err
	}
	return ret, nil
}

// @format
// "hex(from)","hex(to)","type","identity","hex(coin)","value","number","index","hex(tx)","timestamp","hex(tx_from)","hex(tx_to)","hex(gp)","hex(func)","token_price"
const csvLineParts = 15

type Res_Transaction_CSVLine string

func (r Res_Transaction_CSVLine) Decode() (*model.Transfer, error) {
	line := string(r)
	reader := csv.NewReader(strings.NewReader(line))
	parts, err := reader.Read()
	if err != nil {
		return nil, err
	}
	if len(parts) != csvLineParts {
		return nil, fmt.Errorf("invalid line: %d: %s", len(parts), line)
	}
	for i := range parts {
		parts[i] = strings.TrimPrefix(parts[i], "\"")
		parts[i] = strings.TrimSuffix(parts[i], "\"")
	}
	obj := &Res_Transaction_String{
		From:      parts[0],
		To:        parts[1],
		Coin:      parts[4],
		Value:     parts[5],
		Block:     parts[6],
		TxHash:    parts[8],
		Timestamp: parts[9],
		TxFrom:    parts[10],
		TxTo:      parts[11],
	}
	if ttype, err := strconv.ParseUint(parts[2], 10, 32); err != nil {
		return nil, err
	} else {
		obj.Type = uint32(ttype)
	}
	if identity, err := strconv.ParseUint(parts[3], 10, 32); err != nil {
		return nil, err
	} else {
		obj.Identity = uint32(identity)
	}
	if index, err := strconv.ParseUint(parts[7], 10, 32); err != nil {
		return nil, err
	} else {
		obj.Index = uint32(index)
	}
	return obj.Decode()
}

// @format
// block, timestamp, index, hex(tx), string(coin), string(from), string(to), string_base10(value), float64(usd_value)
// "2025-12-12 01:48:12"
const csvLinePartsVer2 = 8

type Res_Transaction_CSVLine_Ver2 string

func (r Res_Transaction_CSVLine_Ver2) Decode() (*model.Transfer, error) {
	line := string(r)
	reader := csv.NewReader(strings.NewReader(line))
	parts, err := reader.Read()
	if err != nil {
		return nil, err
	}
	if len(parts) != csvLinePartsVer2 {
		return nil, fmt.Errorf("invalid line: %d: %s", len(parts), line)
	}
	ret := &model.Transfer{}
	var block, index uint64
	if block, err = strconv.ParseUint(parts[0], 10, 64); err != nil {
		return nil, err
	}
	if index, err = strconv.ParseUint(parts[2], 10, 32); err != nil {
		return nil, err
	}
	ret.Pos = block<<16 | index

	// Parse timestamp like "2025-12-12 01:48:12" and convert to RFC3339.
	if ts, err := time.ParseInLocation("2006-01-02 15:04:05", parts[1], time.UTC); err != nil {
		return nil, fmt.Errorf("parse timestamp: %w", err)
	} else {
		ret.Timestamp = ts.Format(time.RFC3339)
	}

	if hash, err := common.HexToHash(parts[3]); err == nil {
		ret.TxHash = hash
	} else {
		return nil, err
	}
	ret.Token = model.HexToAddress(parts[4])
	ret.From = model.HexToAddress(parts[5])
	ret.To = model.HexToAddress(parts[6])
	if value, success := big.NewInt(0).SetString(parts[7], 10); success {
		ret.Value = (*hexutil.Big)(value)
	} else {
		return nil, fmt.Errorf("invalid value: %s", parts[7])
	}

	ret.Type = uint16(model.TransferTypeEvent)
	ret.Txid = 1

	return ret, nil
}

// @format
// int(block), int(index), string(from), string(to), float(usd_value)
const csvLinePartsVer3 = 5

type Res_Transaction_CSVLine_Ver3 string

func (r Res_Transaction_CSVLine_Ver3) Decode() (*model.Transfer, error) {
	line := string(r)
	reader := csv.NewReader(strings.NewReader(line))
	parts, err := reader.Read()
	if err != nil {
		return nil, err
	}
	if len(parts) != csvLinePartsVer3 {
		return nil, fmt.Errorf("invalid line: %d: %s", len(parts), line)
	}
	ret := &model.Transfer{}
	var block, index uint64
	if block, err = strconv.ParseUint(parts[0], 10, 64); err != nil {
		return nil, err
	}
	if index, err = strconv.ParseUint(parts[1], 10, 32); err != nil {
		return nil, err
	}
	ret.Pos = block<<16 | index
	ret.From = model.HexToAddress(parts[2])
	ret.To = model.HexToAddress(parts[3])
	ret.Token = utils.USDTAddress
	if usd_value, err := strconv.ParseFloat(parts[4], 64); err != nil {
		return nil, err
	} else {
		usd_value_int, _ := big.NewFloat(usd_value * math.Pow10(model.DollarDecimals)).Int(nil)
		ret.Value = (*hexutil.Big)(usd_value_int)
	}
	return ret, nil
}

type Res_ConditionTokenTransaction struct {
	Total        int                `json:"Total"`
	Transactions []*Res_Transaction `json:"Transactions"`
	NewOffset    int                `json:"NewOffset"`
}

type Req_TokenTransaction struct {
	ChainId int    `json:"ChainId"`
	Block   uint64 `json:"Number"`
}

type Res_TokenTransaction struct {
	ChainId      int                `json:"ChainId"`
	Blocks       []uint64           `json:"Numbers"`
	Transactions []*Res_Transaction `json:"Transactions"`
}

func Query_ConditionTokenTransaction(initReq *Req_ConditionTokenTransaction, totalLimit int) ([]*model.Transfer, error) {
	ret := make([]*model.Transfer, 0)
	if totalLimit <= 0 {
		totalLimit = GLOBAL_TOTAL_LIMIT
	}
	newOffset := 0
	for len(ret) < totalLimit {
		initReq.Offset = newOffset
		if totalLimit-len(ret) < GLOBAL_SINGLE_LIMIT {
			initReq.Limit = totalLimit - len(ret)
		} else {
			initReq.Limit = GLOBAL_SINGLE_LIMIT
		}
		thisSingleLimit := initReq.Limit
		data, err := json.Marshal(initReq)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest("GET", URL_ConditionTokenTransaction, bytes.NewBuffer(data))
		if err != nil {
			return nil, err
		}
		req.Header.Add("Content-Type", "application/json")
		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		resBody := &Res_ConditionTokenTransaction{}
		err = json.NewDecoder(res.Body).Decode(resBody)
		if err != nil {
			return nil, err
		}
		if resBody.Total == 0 {
			break
		} else {
			if retTransfers, err := convertConditionTokenTransaction(resBody.Transactions, nil); err != nil {
				return nil, err
			} else {
				ret = append(ret, retTransfers...)
			}
			if resBody.Total < thisSingleLimit {
				break
			} else {
				newOffset = resBody.NewOffset
			}
		}
	}
	return ret, nil
}

func Query_ConditionTokenTransactionPURL(req *Req_ConditionTokenTransaction) (*Res_ConditionTokenTransaction, error) {
	/*
		u := url.URL{
			Scheme: URL_SCHEME,
			Host:   URL_HOST,
			Path:   URL_ConditionTokenTransaction_PATH,
			RawQuery: url.Values{
				"ChainId":        {fmt.Sprintf("%d", req.ChainId)},
				"To":             {req.To},
				"Coin":           {req.Coin},
				"StartTimestamp": {req.StartTimestamp},
				"Limit":          {fmt.Sprintf("%d", req.Limit)},
				"Offset":         {fmt.Sprintf("%d", 0)},
			}.Encode(),
		}
	*/
	return nil, nil
}

func Query_TokenTransaction(req *Req_TokenTransaction) (*Res_TokenTransaction, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	reqHttp, err := http.NewRequest("GET", URL_TokenTransaction, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	reqHttp.Header.Add("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(reqHttp)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resBody := &Res_TokenTransaction{}
	err = json.NewDecoder(res.Body).Decode(resBody)
	if err != nil {
		return nil, err
	}
	return resBody, nil
}

func Query_TokenTransactionPURL(req *Req_TokenTransaction) (*Res_TokenTransaction, error) {
	u := url.URL{
		Scheme: URL_SCHEME,
		Host:   URL_HOST,
		Path:   URL_TokenTransaction_PATH,
		RawQuery: url.Values{
			"ChainId": {fmt.Sprintf("%d", req.ChainId)},
			"Number":  {fmt.Sprintf("%d", req.Block)},
		}.Encode(),
	}
	reqHttp, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{}
	res, err := client.Do(reqHttp)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	resBody := &Res_TokenTransaction{}
	err = json.NewDecoder(res.Body).Decode(resBody)
	if err != nil {
		return nil, err
	}
	return resBody, nil
}

type TokenTransaction_BatchConfig struct {
	ChainId    int
	Blocks     []uint64
	EverySleep int           // Number of blocks to process before sleeping
	Sleep      time.Duration // Duration to sleep after processing every `EverySleep` blocks

	TokenFilter map[string]struct{} // Only return transfers involving these tokens
}

func Query_TokenTransaction_BlockBatch(config *TokenTransaction_BatchConfig) ([]*model.Transfer, error) {
	ret := make([]*model.Transfer, 0)
	cache := make([]*Res_Transaction, 0)
	for i, block := range config.Blocks {
		req := &Req_TokenTransaction{
			ChainId: config.ChainId,
			Block:   block,
		}
		transfers, err := Query_TokenTransactionPURL(req)
		if err != nil {
			return nil, err
		}
		cache = append(cache, transfers.Transactions...)
		if i > 0 && i%config.EverySleep == 0 {
			time.Sleep(config.Sleep)
			cachedTransfers, err := convertConditionTokenTransaction(cache, config.TokenFilter)
			if err != nil {
				return nil, err
			}
			ret = append(ret, cachedTransfers...)
			cache = make([]*Res_Transaction, 0) // Clear cache after processing
		}
	}
	if len(cache) > 0 {
		// Process any remaining transfers in the cache
		cachedTransfers, err := convertConditionTokenTransaction(cache, config.TokenFilter)
		if err != nil {
			return nil, err
		}
		ret = append(ret, cachedTransfers...)
		cache = make([]*Res_Transaction, 0) // Clear cache after processing
	}
	return ret, nil
}

func Query_TokenTransaction_BlockBatch_Parallel(configs []*TokenTransaction_BatchConfig, parallel int) ([]*model.Transfer, error) {
	var mu sync.Mutex
	var ret []*model.Transfer
	query := func(i int) error {
		thisRet, err := Query_TokenTransaction_BlockBatch(configs[i])
		if err != nil {
			return err
		}
		mu.Lock()
		ret = append(ret, thisRet...)
		mu.Unlock()
		return nil
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for i := range configs {
		i := i // capture range variable
		eg.Go(func() error {
			return query(i)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return ret, nil
}

const DEBUG_HOOKED = true

func convertConditionTokenTransaction(rt []*Res_Transaction, tokenFilter map[string]struct{}) ([]*model.Transfer, error) {
	var err error
	transfers := make([]*model.Transfer, 0, len(rt))
	for _, tx := range rt {
		if DEBUG_HOOKED {
			if strings.Compare(tx.Coin, "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t") != 0 {
				continue // Only process USDT transfers
			}
		}
		var token model.Address
		if strings.Compare(tx.Coin, model.NativeTokenNotation) == 0 {
			// If the token is the native token, use the native token address
			token = model.NativeTokenAddress
		} else {
			token = model.HexToAddress(tx.Coin)
		}
		if tokenFilter != nil {
			// If no token filter is provided, include all tokens}
			if _, ok := tokenFilter[utils.AddrToAddrString(token)]; !ok {
				continue
			}
		}
		transfer := &model.Transfer{
			Pos:       uint64(tx.Block)<<16 | uint64(tx.Index),
			Txid:      uint16(tx.Identity),
			Type:      uint16(tx.Type),
			Timestamp: tx.Timestamp,
		}
		transfer.From = model.HexToAddress(tx.From)
		transfer.To = model.HexToAddress(tx.To)
		transfer.Token = token
		if value, success := big.NewInt(0).SetString(tx.Value, 10); success {
			transfer.Value = (*hexutil.Big)(value)
		} else {
			continue
		}
		if transfer.TxHash, err = common.HexToHash(tx.TxHash); err != nil {
			continue
		}
		transfers = append(transfers, transfer)
	}
	return transfers, nil
}
