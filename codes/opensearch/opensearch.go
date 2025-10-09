package opensearch

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"time"
	"transfer-graph/model"
	"transfer-graph/utils"

	"github.com/ethereum/go-ethereum/log"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zstd"
	opensearch "github.com/opensearch-project/opensearch-go/v2"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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
	if err != nil {
		return fmt.Errorf("close file failed: %s", err.Error())
	}
	return nil
}

func LoadQueryResultLegacy(filePath string) (*model.QueryResult, error) {
	p := filePath
	f, err := os.Open(p)
	if err != nil {
		return nil, fmt.Errorf("open file failed (file: %s): %s", p, err.Error())
	}
	buf := bytes.NewBuffer(nil)
	if err := utils.Decompress(f, buf); err != nil {
		return nil, fmt.Errorf("decompress failed: %s", err.Error())
	}

	s := &model.QueryResult{}
	if err := json.NewDecoder(buf).Decode(s); err != nil {
		return nil, fmt.Errorf("json decode failed: %s", err.Error())
	}
	return s, nil
}

type OpenSearchConfig struct {
	Index    string `toml:"index"`
	Url      string `toml:"url"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

func queryOpenSearch(ctx context.Context, body string, config *OpenSearchConfig) (*model.QueryResult, error) {
	index := config.Index
	url := config.Url
	user := config.User
	password := config.Password

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{url},
		Username:  user,
		Password:  password,
	})
	if err != nil {
		return nil, err
	}
	search := opensearchapi.SearchRequest{
		Index: []string{index},
		Body:  bytes.NewBufferString(body),
	}
	resp, err := search.Do(ctx, client)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response error: status code = %d, response = %s", resp.StatusCode, resp.String())
	}

	type R struct {
		Aggregations struct {
			Transfer struct {
				Value *model.QueryResult `json:"value"`
			} `json:"transfer"`
		} `json:"aggregations"`
	}

	var r R
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	result := r.Aggregations.Transfer.Value

	// Process ERC1155
	for _, t := range result.Transfers {
		if t.Type == uint16(model.TransferTypeERC1155Single) || t.Type == uint16(model.TransferTypeERC1155Batch) {
			data := t.Extras["data"].(string)
			operator := t.Extras["operator"].(string)
			m, err := model.TransformERC1155(t.Type, operator, data)
			if err == nil {
				t.Extras = m
			} else {
				log.Info("decode ERC-1155 failed", "block", t.Block(), "index", t.Index(), "err", err.Error())
				t.Extras = make(map[string]interface{})
			}
		}
	}
	return result, nil
}

func QueryOpenSearch(startBlock, endBlock, step uint64, config *OpenSearchConfig) ([]*model.QueryResult, error) {
	a := make([]*model.QueryResult, 0)
	for i := startBlock; i < endBlock; i += step {
		start := i
		end := i + step
		if end > endBlock {
			end = endBlock
		}

		var wNativeAddr string
		switch config.Index {
		case "eth_block":
			wNativeAddr = WETH
		case "bsc_block":
			wNativeAddr = WBNB
		default:
			log.Crit("unknown index", "index", config.Index)
		}
		dsl := dslGetTransfer(uint64(start), uint64(end), wNativeAddr)
		s, err := queryOpenSearch(context.Background(), dsl, config)
		if err != nil {
			return nil, err
		}
		a = append(a, s)
	}
	return a, nil
}

func DumpDataFromOpenSearch(startBlock, endBlock, step uint64, datadir string, config *OpenSearchConfig, stats *Statistics) {
	for i := startBlock; i < endBlock; i += step {
		start := i
		end := i + step
		if end > endBlock {
			end = endBlock
		}

		var wNativeAddr string
		switch config.Index {
		case "eth_block":
			wNativeAddr = WETH
		case "bsc_block":
			wNativeAddr = WBNB
		default:
			log.Crit("unknown index", "index", config.Index)
		}
		dsl := dslGetTransfer(uint64(start), uint64(end), wNativeAddr)
		startTime := time.Now()
		s, err := queryOpenSearch(context.Background(), dsl, config)
		if err != nil {
			log.Crit("query failed", "err", err.Error())
		}

		realEnd := end - 1
		log.Info("query done", "start", start, "end", realEnd, "transfers", len(s.Transfers), "txs", len(s.Txs), "duration", time.Since(startTime))

		if len(s.Transfers) == 0 && len(s.Txs) == 0 {
			continue
		}
		p := path.Join(datadir, fmt.Sprintf("%d_%d.json.zst", start, realEnd))
		err = DumpQueryResult(p, s)
		if err != nil {
			log.Crit("dump failed", "err", err.Error())
		}
		stats.Dump(s)
	}
}

func GetLatestBlockFromOpenSearch(config *OpenSearchConfig) (uint64, error) {
	type Result struct {
		Took     int  `json:"took"`
		TimedOut bool `json:"timed_out"`
		Shards   struct {
			Total      int `json:"total"`
			Successful int `json:"successful"`
			Skipped    int `json:"skipped"`
			Failed     int `json:"failed"`
		} `json:"_shards"`
		Hits struct {
			Total struct {
				Value    int    `json:"value"`
				Relation string `json:"relation"`
			} `json:"total"`
			MaxScore interface{} `json:"max_score"`
			Hits     []struct {
				Index  string      `json:"_index"`
				Type   string      `json:"_type"`
				ID     string      `json:"_id"`
				Score  interface{} `json:"_score"`
				Source struct {
					Number uint64 `json:"Number"`
				} `json:"_source"`
				Sort []int `json:"sort"`
			} `json:"hits"`
		} `json:"hits"`
	}
	dsl := `{"_source":"Number","query":{"match_all":{}},"size":1,"sort":[{"Number":{"order":"desc"}}]}`

	index := config.Index
	url := config.Url
	user := config.User
	password := config.Password

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{url},
		Username:  user,
		Password:  password,
	})
	if err != nil {
		return 0, err
	}
	search := opensearchapi.SearchRequest{
		Index: []string{index},
		Body:  bytes.NewBufferString(dsl),
	}
	resp, err := search.Do(context.Background(), client)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("response error: status code = %d, response = %s", resp.StatusCode, resp.String())
	}
	var r Result
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, err
	}
	return r.Hits.Hits[0].Source.Number, nil
}

func GetLatestBlockFromOpenSearchV2(config *OpenSearchConfig) (uint64, error) {
	type Result struct {
		Took     int  `json:"took"`
		TimedOut bool `json:"timed_out"`
		Shards   struct {
			Total      int `json:"total"`
			Successful int `json:"successful"`
			Skipped    int `json:"skipped"`
			Failed     int `json:"failed"`
		} `json:"_shards"`
		Hits struct {
			Total struct {
				Value    int    `json:"value"`
				Relation string `json:"relation"`
			} `json:"total"`
			MaxScore float64 `json:"max_score"`
			Hits     []struct {
				Index  string  `json:"_index"`
				Type   string  `json:"_type"`
				ID     string  `json:"_id"`
				Score  float64 `json:"_score"`
				Source struct {
					Value int `json:"Value"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	keys := []string{"ETHHighestOrderlyBlock", "BSCHighestOrderlyBlock"}
	dsl := `{"query":{"match_all":{}}}`

	url := config.Url
	user := config.User
	password := config.Password

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{url},
		Username:  user,
		Password:  password,
	})
	if err != nil {
		return 0, err
	}
	search := opensearchapi.SearchRequest{
		Index: []string{"global"},
		Body:  bytes.NewBufferString(dsl),
	}
	resp, err := search.Do(context.Background(), client)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("response error: status code = %d, response = %s", resp.StatusCode, resp.String())
	}
	var r Result
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, err
	}

	_ = keys
	panic("not implemented")
}
