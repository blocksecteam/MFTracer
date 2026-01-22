package data

import (
	"math"
	"math/big"
	"time"
	"transfer-graph-evm/model"
	"transfer-graph-evm/utils"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/fbsobreira/gotron-sdk/pkg/common"
)

// AwsTransferRow matches the file schema whose root group name is "Schema".
// The leading anonymous field tags the root so parquet-go maps columns under
// the correct root instead of the default Parquet_go_root.
type AwsTransferRow struct {
	Schema         struct{}
	From_address   string
	To_address     string
	Transfer_type  uint32
	Token_address  string
	Amount         string
	Block_number   uint64
	Tx_inner_index uint32
	Tx_index       uint32
	Tx_hash        string
	Timestamp      uint64
	Tx_to_address  string
	Usd_value      float64
}

func (r *AwsTransferRow) Decode() *model.Transfer {
	ret := &model.Transfer{
		Pos:    model.MakeTransferPos(uint64(r.Block_number), uint16(r.Tx_index)),
		Txid:   uint16(r.Tx_inner_index),
		Type:   uint16(r.Transfer_type),
		From:   model.BytesToAddress([]byte(r.From_address)),
		To:     model.BytesToAddress([]byte(r.To_address)),
		Token:  utils.USDTAddress,
		TxHash: common.BytesToHash([]byte(r.Tx_hash)),
	}
	usdValueInt, _ := big.NewFloat(r.Usd_value * math.Pow10(model.DollarDecimals)).Int(nil)
	ret.Value = (*hexutil.Big)(usdValueInt)
	ret.Timestamp = time.UnixMilli(int64(r.Timestamp)).Format(time.RFC3339)
	return ret
}
