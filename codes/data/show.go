package data

import "transfer-graph-evm/model"

type ReadableTransfer struct {
	Block     uint64 `json:"block"`
	Index     uint16 `json:"index"`
	Txid      uint16 `json:"txid"`
	Type      uint16 `json:"type"`
	From      string `json:"from"`
	To        string `json:"to"`
	Token     string `json:"token"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
	TxHash    string `json:"tx_hash"`
}

func ShowTransfer(ts *model.Transfer) (string, error) {
	rt := ReadableTransfer{
		Block:  ts.Block(),
		Index:  ts.Index(),
		Txid:   ts.Txid,
		Type:   ts.Type,
		From:   ts.From.String(),
		To:     ts.To.String(),
		Token:  ts.Token.String(),
		TxHash: ts.TxHash.String(),
	}
	rt.Value = ts.Value.ToInt().Text(10)
	if b, err := json.MarshalIndent(rt, "", "  "); err != nil {
		return "", err
	} else {
		return string(b), nil
	}
}
