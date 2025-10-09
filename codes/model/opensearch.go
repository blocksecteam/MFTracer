package model

type QueryResult struct {
	Transfers []*Transfer `json:"transfers"`
	Txs       []*Tx       `json:"txs"`
}
