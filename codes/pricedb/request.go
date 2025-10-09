package pricedb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

const priceServiceUrl string = "http://localhost:7001/api/tokenpricebulkV2"

type requestInfo struct {
	Amount string `json:"amount"`
}

type requestToken struct {
	Address string        `json:"address"`
	Info    []requestInfo `json:"info"`
}

type requestBody struct {
	ChainId     int            `json:"chainid"`
	Tokens      []requestToken `json:"tokens"`
	BlockNumber string         `json:"blocknumber"`
	Timestamp   int            `json:"timestamp"`
	Advanced    bool           `json:"advanced"`
}

type responseInfo struct {
	Amount string `json:"amount"`
	Value  string `json:"value"`
}

type responsePrice struct {
	Address string         `json:"address"`
	Info    []responseInfo `json:"info"`
}

type responseBody struct {
	Prices  []responsePrice `json:"Prices"`
	Message string          `json:"message"`
}

func FetchPrice(block, timestamp uint64, tokens []common.Address) ([]float64, error) {
	dataStruct := requestBody{
		ChainId:     1,
		Tokens:      make([]requestToken, len(tokens)),
		BlockNumber: strconv.Itoa(int(block)),
		Timestamp:   int(timestamp),
		Advanced:    true,
	}
	for i, token := range tokens {
		dataStruct.Tokens[i].Address = strings.ToLower(token.Hex())
		dataStruct.Tokens[i].Info = make([]requestInfo, 1)
		dataStruct.Tokens[i].Info[0] = requestInfo{
			Amount: strconv.Itoa(PriceFactor),
		}
	}
	data, err := json.Marshal(dataStruct)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", priceServiceUrl, bytes.NewBuffer(data))
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
	resBody := &responseBody{}
	err = json.NewDecoder(res.Body).Decode(resBody)
	if err != nil {
		return nil, err
	}
	if strings.Compare(resBody.Message, "Success") != 0 {
		return nil, fmt.Errorf("response body.message != \"Success\"")
	}
	ret := make([]float64, len(tokens))
	for i := range resBody.Prices {
		if strings.Compare(resBody.Prices[i].Address, dataStruct.Tokens[i].Address) != 0 {
			return nil, fmt.Errorf("order error")
		}
		ret[i], err = strconv.ParseFloat(resBody.Prices[i].Info[0].Value, 64)
		if err != nil {
			fmt.Println(resBody.Prices[i].Address, dataStruct.BlockNumber, dataStruct.Timestamp)
			return nil, err
		}
	}
	return ret, nil
}

func FetchPriceMultiReq(block, timestamp uint64, tokens []common.Address, tries int, ceaseTime time.Duration) ([]float64, error) {
	dataStructs := make([]*requestBody, len(tokens))
	for i, token := range tokens {
		dataStruct := &requestBody{
			ChainId:     1,
			Tokens:      make([]requestToken, 1),
			BlockNumber: strconv.Itoa(int(block)),
			Timestamp:   int(timestamp),
			Advanced:    true,
		}
		dataStruct.Tokens[0].Address = strings.ToLower(token.Hex())
		dataStruct.Tokens[0].Info = make([]requestInfo, 1)
		dataStruct.Tokens[0].Info[0] = requestInfo{
			Amount: strconv.Itoa(PriceFactor),
		}
		dataStructs[i] = dataStruct
	}
	ret := make([]float64, len(tokens))
	for i, dataStruct := range dataStructs {
		data, err := json.Marshal(dataStruct)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest("POST", priceServiceUrl, bytes.NewBuffer(data))
		if err != nil {
			return nil, err
		}
		req.Header.Add("Content-Type", "application/json")
		resBody := &responseBody{}
		price := ""
		try := 0
		for ; strings.Compare(price, "") == 0 && try < tries; try++ {
			client := &http.Client{}
			res, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			resBody.Message = ""
			resBody.Prices = nil
			err = json.NewDecoder(res.Body).Decode(resBody)
			if err != nil {
				return nil, err
			}
			if strings.Compare(resBody.Message, "Success") != 0 {
				return nil, fmt.Errorf("response body.message != \"Success\"")
			}
			if strings.Compare(resBody.Prices[0].Address, dataStruct.Tokens[0].Address) != 0 {
				return nil, fmt.Errorf("order error")
			}
			price = resBody.Prices[0].Info[0].Value
			res.Body.Close()
			time.Sleep(ceaseTime)
			fmt.Println("[Debug] one try:", dataStruct.Tokens[0].Address)
		}
		if try == tries {
			ret[i] = 0
		} else {
			ret[i], err = strconv.ParseFloat(price, 64)
			if err != nil {
				return nil, err
			}
		}
		//time.Sleep(ceaseTime)
	}
	return ret, nil
}

func FetchPriceRetry(block, timestamp uint64, tokens []common.Address, tries int, ceaseTime time.Duration) ([]float64, error) {
	type indexedToken struct {
		address string
		index   int
	}
	dataStruct := requestBody{
		ChainId:     1,
		BlockNumber: strconv.Itoa(int(block)),
		Timestamp:   int(timestamp),
		Advanced:    true,
	}
	defaultInfo := []requestInfo{{
		Amount: strconv.Itoa(PriceFactor),
	}}
	remainTokens := make([]indexedToken, len(tokens))
	for i, token := range tokens {
		remainTokens[i] = indexedToken{
			address: strings.ToLower(token.Hex()),
			index:   i,
		}
	}
	ret := make([]float64, len(tokens))

	for try := 0; try < tries && len(remainTokens) > 0; try++ {
		dataStruct.Tokens = make([]requestToken, len(remainTokens))
		for i, token := range remainTokens {
			dataStruct.Tokens[i] = requestToken{
				Address: token.address,
				Info:    defaultInfo,
			}
		}
		data, err := json.Marshal(dataStruct)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest("POST", priceServiceUrl, bytes.NewBuffer(data))
		if err != nil {
			return nil, err
		}
		req.Header.Add("Content-Type", "application/json")
		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		resBody := &responseBody{}
		err = json.NewDecoder(res.Body).Decode(resBody)
		if err != nil {
			return nil, err
		}
		if strings.Compare(resBody.Message, "Success") != 0 {
			return nil, fmt.Errorf("response body.message != \"Success\"")
		}
		newRemainTokens := make([]indexedToken, 0, len(remainTokens))
		for i := range resBody.Prices {
			if strings.Compare(resBody.Prices[i].Address, dataStruct.Tokens[i].Address) != 0 {
				return nil, fmt.Errorf("order error")
			}
			value := resBody.Prices[i].Info[0].Value
			if strings.Compare(value, "") == 0 {
				newRemainTokens = append(newRemainTokens, indexedToken{
					address: remainTokens[i].address,
					index:   remainTokens[i].index,
				})
			} else {
				ret[remainTokens[i].index], err = strconv.ParseFloat(value, 64)
				if err != nil {
					return nil, err
				}
			}
		}
		remainTokens = newRemainTokens
		res.Body.Close()
		time.Sleep(ceaseTime)
		fmt.Println("[Debug] one try finished, len(remain) = ", len(remainTokens))
	}
	return ret, nil
}
