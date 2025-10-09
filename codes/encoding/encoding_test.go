package encoding

import (
	"fmt"
	"testing"
	"transfer-graph/model"
	"transfer-graph/opensearch"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
)

func EncodeRLP(q *model.QueryResult) common.StorageSize {
	var total common.StorageSize
	for _, t := range q.Transfers {
		s, err := EncodeTransferRlp(t)
		if err != nil {
			panic(err)
		}
		total += common.StorageSize(len(s))
		_, err = DecodeTransferRlp(s)
		if err != nil {
			panic(err)
		}
	}
	for _, t := range q.Txs {
		s, err := EncodeTxRlp(t)
		if err != nil {
			panic(err)
		}
		total += common.StorageSize(len(s))
		_, err = DecodeTxRlp(s)
		if err != nil {
			panic(err)
		}
	}
	return total
}

func EncodeMsgp(q *model.QueryResult) common.StorageSize {
	var total common.StorageSize
	for _, t := range q.Transfers {
		s, err := EncodeTransferMsgp(t)
		if err != nil {
			panic(err)
		}
		total += common.StorageSize(len(s))
		_, _, err = DecodeTransferMsgp(s)
		if err != nil {
			panic(err)
		}
	}
	for _, t := range q.Txs {
		s, err := EncodeTxMsgp(t)
		if err != nil {
			panic(err)
		}
		total += common.StorageSize(len(s))
		_, _, err = DecodeTxMsgp(s)
		if err != nil {
			panic(err)
		}
	}
	return total
}

func BenchmarkEncodeDecodeRLP(b *testing.B) {
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, err := opensearch.LoadQueryResult("./data_eth/13000000_13005000.json.zstd")
		require.NoError(b, err)
		b.StartTimer()
		fmt.Println(EncodeRLP(q))
	}
}

func BenchmarkEncodeDecodeMsgp(b *testing.B) {
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q, err := opensearch.LoadQueryResult("./data_eth/13000000_13005000.json.zstd")
		require.NoError(b, err)
		b.StartTimer()
		fmt.Println(EncodeMsgp(q))
	}
}
