package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
	"transfer-graph/encoding"
	"transfer-graph/model"
	"transfer-graph/search"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/log"
	cmap "github.com/orcaman/concurrent-map/v2"
	"golang.org/x/sync/errgroup"
)

func getQueryMetrics(ctx context.Context, config *QueryConfig) *QueryMetrics {
	var metrics *QueryMetrics
	if v := ctx.Value(QueryMetricsKey); v != nil && config.DetailedMetrics {
		metrics = v.(*QueryMetrics)
	}
	return metrics
}

type Filter interface {
	Type(uint16) bool
	Value(string) bool
}

type EmptyFilter struct{}

func (f *EmptyFilter) Type(_ uint16) bool {
	return true
}

func (f *EmptyFilter) Value(_ string) bool {
	return true
}

type JsonFilter struct {
	Value_ *string `json:"value,omitempty"`
	Type_  *uint16 `json:"type,omitempty"`
}

func (f *JsonFilter) Type(t uint16) bool {
	if f.Type_ != nil && t != *f.Type_ {
		return false
	}
	return true
}

func (f *JsonFilter) Value(v string) bool {
	if f.Value_ != nil && strings.Compare(v, *f.Value_) == 1 {
		return false
	}
	return true
}

type QueryConfig struct {
	DetailedMetrics bool        `json:"detailedMetrics,omitempty"`
	BatchSize       uint64      `json:"batchSize,omitempty"`
	JsonFilter      *JsonFilter `json:"filter,omitempty"`
	CountUpperBound uint64      `json:"countUpperBound,omitempty"`

	CountLimit   uint64 `json:"countLimit,omitempty"`
	Timeout      uint64 `json:"timeout,omitempty"`
	SizeLimit    uint64 `json:"sizeLimit,omitempty"`
	FetchThreads uint64 `json:"fetchThreads,omitempty"`

	queryStartTime time.Time `json:"-"`
	totalSize      uint64    `json:"-"`
	filter         Filter    `json:"-"`
}

func (c *QueryConfig) String() string {
	data, _ := json.Marshal(c)
	return string(data)
}

func (c *QueryConfig) GetSizeLimit() uint64 {
	if c.SizeLimit == 0 {
		return math.MaxUint
	}
	return c.SizeLimit * 1024 * 1024
}

func (c *QueryConfig) GetTimeout() time.Duration {
	if c.Timeout == 0 {
		return math.MaxInt64
	}
	return time.Duration(c.Timeout) * time.Second
}

func (c *QueryConfig) GetCountLimit() uint64 {
	if c.CountLimit == 0 {
		return math.MaxUint32
	}
	return c.CountLimit
}

func (c *QueryConfig) GetCountUpperBound() uint64 {
	if c.CountUpperBound == 0 {
		return math.MaxUint32
	}
	return c.CountUpperBound
}

func (q *QueryConfig) Filter() Filter {
	if q.filter != nil {
		return q.filter
	}
	if q.JsonFilter == nil {
		q.filter = &EmptyFilter{}
	} else {
		q.filter = q.JsonFilter
	}
	return q.filter
}

func (q *QueryConfig) GetBatchSize() uint64 {
	if q.BatchSize == 0 {
		return 0
	}
	return q.BatchSize
}

func (q *QueryConfig) GetFetchThreads() uint64 {
	if q.FetchThreads == 0 {
		return 1
	}
	return q.FetchThreads
}

func DefaultQueryConfig() *QueryConfig {
	return &QueryConfig{
		FetchThreads:    4,
		DetailedMetrics: false,

		queryStartTime: time.Now(),
	}
}

func (g *GraphDB) gidToSubgraph(ctx context.Context, gid []byte, config *QueryConfig) (*model.Subgraph, error) {
	metrics := getQueryMetrics(ctx, config)
	recordMetrics := metrics != nil

	totalSize := uint64(0)
	startTime := time.Now()
	defer func(start time.Time) {
		d := time.Since(startTime)
		if recordMetrics {
			metrics.AddSubgraph(d, int(totalSize), 1)
		}
	}(startTime)

	start := time.Now()
	v, err := g.db.Get(gid)
	if err != nil {
		return nil, err
	}
	if recordMetrics {
		metrics.AddDBGet(time.Since(start), len(v))
	}

	t, err := encoding.DefaultEncoding.DecodeSubgraph(v)
	if err != nil {
		err = fmt.Errorf("decode gid=%s failed: %s", hexutil.Encode([]byte(gid)), err.Error())
		return nil, err
	}
	totalSize += uint64(len(v))
	return t, nil
}

func (g *GraphDB) gidsToSubgraphs(ctx context.Context, gids map[string]struct{}, config *QueryConfig) (map[string]*model.Subgraph, error) {
	metrics := getQueryMetrics(ctx, config)
	recordMetrics := metrics != nil

	totalSize := uint64(0)
	startTime := time.Now()
	defer func(start time.Time) {
		d := time.Since(startTime)
		if recordMetrics {
			metrics.AddSubgraph(d, int(totalSize), len(gids))
		}
	}(startTime)

	timeLimit := config.GetTimeout()
	sizeLimit := config.GetSizeLimit()
	ret := make(map[string]*model.Subgraph)
	for gid := range gids {
		if _, ok := ret[gid]; ok {
			continue
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("interrupted")
		default:
		}
		if time.Now().After(startTime.Add(timeLimit)) {
			return nil, fmt.Errorf("time limit exceeded in gidsToSubgraphs: %s, total subgraphs = %d, total size = %d",
				common.PrettyDuration(timeLimit).String(), len(ret), totalSize)
		}

		start := time.Now()
		v, err := g.db.Get([]byte(gid))
		if err != nil {
			return nil, err
		}
		if recordMetrics {
			metrics.AddDBGet(time.Since(start), len(v))
		}
		t, err := encoding.DefaultEncoding.DecodeSubgraph(v)
		if err != nil {
			err = fmt.Errorf("decode gid=%s failed: %s", hexutil.Encode([]byte(gid)), err.Error())
			return nil, err
		}
		ret[gid] = t
		totalSize += uint64(len(v))
		if totalSize > sizeLimit {
			return nil, fmt.Errorf("size limit exceeded: %d > %d, total subgraphs = %d",
				totalSize, sizeLimit, len(ret))
		}
	}
	return ret, nil
}

func (g *GraphDB) gidsToSubgraphsParallel(ctx context.Context, gids map[string]struct{}, parallel int, config *QueryConfig) (map[string]*model.Subgraph, error) {
	metrics := getQueryMetrics(ctx, config)
	recordMetrics := metrics != nil

	totalSize := uint64(0)
	startTime := time.Now()
	defer func(start time.Time) {
		d := time.Since(startTime)
		if recordMetrics {
			metrics.AddSubgraph(d, int(totalSize), len(gids))
		}
	}(startTime)

	ret := cmap.New[*model.Subgraph]()
	step := func(gid string) error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		default:
		}
		ret.Set(gid, nil)

		start := time.Now()
		v, err := g.db.Get([]byte(gid))
		if err != nil {
			return err
		}
		if recordMetrics {
			metrics.AddDBGet(time.Since(start), len(v))
		}

		t, err := encoding.DefaultEncoding.DecodeSubgraph(v)
		if err != nil {
			err = fmt.Errorf("decode gid=%s failed: %s", hexutil.Encode([]byte(gid)), err.Error())
			return err
		}
		ret.Set(gid, t)
		return nil
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for gid := range gids {
		s := string(gid)
		if ret.Has(s) {
			continue
		}
		eg.Go(func() error {
			id := s
			return step(id)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return ret.Items(), nil
}

func (g *GraphDB) BlockIDWithTokenToSubgraph(ctx context.Context, blockID uint16, token common.Address, config *QueryConfig) (*model.Subgraph, error) {
	gid := model.MakeGIDWithBlockIDPack(blockID, token)
	ret, err := g.gidToSubgraph(ctx, gid, config)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return nil, err
	}
	if err != nil && errors.Is(err, pebble.ErrNotFound) {
		return nil, fmt.Errorf("subgraph with blockID=%d & token=%s does not exist", blockID, token.Hex())
	}
	return ret, nil
}

func (g *GraphDB) BlockIDWithTokensToSubgraphs(ctx context.Context, blockID uint16, tokens []common.Address, config *QueryConfig) ([]*model.Subgraph, error) {
	blockIDs := make([]uint16, 1)
	blockIDs[0] = blockID
	return g.BlockIDsWithTokensToSubgraphs(ctx, blockIDs, tokens, config)
}

func (g *GraphDB) BlockIDScanAllTokensToSubgraphs(ctx context.Context, blockID uint16, config *QueryConfig) ([]*model.Subgraph, error) {
	gidPrefix := model.MakeGIDPrefixWithBlockID(blockID)
	iter := g.db.NewIterator(gidPrefix, nil)
	defer iter.Release()

	metrics := getQueryMetrics(ctx, config)
	recordMetrics := metrics != nil

	totalSize := uint64(0)
	startTime := time.Now()
	ret := make([]*model.Subgraph, 0)
	defer func(start time.Time) {
		d := time.Since(startTime)
		if recordMetrics {
			metrics.AddSubgraph(d, int(totalSize), len(ret))
		}
	}(startTime)

	sizeLimit := config.GetSizeLimit()
	timeLimit := config.GetTimeout()
	for iter.Next() {
		if err := iter.Error(); err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("interrupted")
		default:
		}
		if time.Now().After(startTime.Add(timeLimit)) {
			return nil, fmt.Errorf("time limit exceeded in gidsToSubgraphs: %s, total subgraphs = %d, total size = %d",
				common.PrettyDuration(timeLimit).String(), len(ret), totalSize)
		}

		v := iter.Value()
		if recordMetrics {
			metrics.AddDBRangeScan(len(v))
		}

		t, err := encoding.DefaultEncoding.DecodeSubgraph(v)
		if err != nil {
			err = fmt.Errorf("decode gid=%s failed: %s", hexutil.Encode(iter.Key()), err.Error())
			return nil, err
		}
		ret = append(ret, t)
		totalSize += uint64(len(v))
		if totalSize > sizeLimit {
			return nil, fmt.Errorf("size limit exceeded: %d > %d, total subgraphs = %d",
				totalSize, sizeLimit, len(ret))
		}
	}

	return ret, nil
}

func (g *GraphDB) BlockIDsWithTokenToSubgraphs(ctx context.Context, blockIDs []uint16, token common.Address, config *QueryConfig) ([]*model.Subgraph, error) {
	tokens := make([]common.Address, 1)
	tokens[0] = token
	return g.BlockIDsWithTokensToSubgraphs(ctx, blockIDs, tokens, config)
}

func (g *GraphDB) BlockIDsWithTokensToSubgraphs(ctx context.Context, blockIDs []uint16, tokens []common.Address, config *QueryConfig) ([]*model.Subgraph, error) {
	gids := make(map[string]struct{}, len(blockIDs)*len(tokens))
	gidsSorted := make([]string, 0, len(blockIDs)*len(tokens))
	for _, blockID := range blockIDs {
		for _, token := range tokens {
			gid := model.MakeGIDWithBlockIDPack(blockID, token)
			gids[string(gid)] = struct{}{}
			gidsSorted = append(gidsSorted, string(gid))
		}
	}
	var m map[string]*model.Subgraph = nil
	var err error
	fetchThreads := config.GetFetchThreads()
	if fetchThreads == 1 {
		m, err = g.gidsToSubgraphs(ctx, gids, config)
	} else {
		m, err = g.gidsToSubgraphsParallel(ctx, gids, int(fetchThreads), config)
	}
	if err != nil {
		return nil, err
	}
	ret := make([]*model.Subgraph, 0, len(m))
	// @@ [Lagency]
	// @ The result is not sorted by blockIDs->tokens by iterating on the map m.
	// @ We rather iterate on gidsSorted to obatin a sorted slice.
	/*
		for _, v := range m {
			ret = append(ret, v)
		}
	*/
	for i := range gidsSorted {
		ret = append(ret, m[gidsSorted[i]])
	}
	return ret, nil
}

func (g *GraphDB) BlockIDsWithTokensToCompositeSubgraphs(ctx context.Context, blockIDs []uint16, tokens []common.Address, config *QueryConfig) ([]*model.Subgraph, error) {
	gids := make(map[string]struct{}, len(blockIDs))
	gidsSorted := make([]string, 0, len(blockIDs))
	for _, blockID := range blockIDs {
		gid := model.MakeCompositeGIDWithBlockID(blockID, tokens)
		gids[string(gid)] = struct{}{}
		gidsSorted = append(gidsSorted, string(gid))
	}
	var m map[string]*model.Subgraph
	var err error
	fetchThreads := config.GetFetchThreads()
	if fetchThreads == 1 {
		m, err = g.gidsToSubgraphs(ctx, gids, config)
	} else {
		m, err = g.gidsToSubgraphsParallel(ctx, gids, int(fetchThreads), config)
	}
	if err != nil {
		return nil, err
	}
	ret := make([]*model.Subgraph, 0, len(m))
	for i := range gidsSorted {
		ret = append(ret, m[gidsSorted[i]])
	}
	return ret, nil
}

func (g *GraphDB) BlockIDRangeWithTokensToSubgraphs(ctx context.Context, start, end uint16, tokens []common.Address, config *QueryConfig) ([]*model.Subgraph, error) {
	blockIDs := make([]uint16, 0, int(end-start))
	for blockID := start; blockID < end; blockID++ {
		blockIDs = append(blockIDs, blockID)
	}
	return g.BlockIDsWithTokensToSubgraphs(ctx, blockIDs, tokens, config)
}

func (g *GraphDB) BlockIDRangeWithTokenToSubgraphs(ctx context.Context, start, end uint16, token common.Address, config *QueryConfig) ([]*model.Subgraph, error) {
	blockIDs := make([]uint16, 0, int(end-start))
	for blockID := start; blockID < end; blockID++ {
		blockIDs = append(blockIDs, blockID)
	}
	tokens := make([]common.Address, 1)
	tokens[0] = token
	return g.BlockIDsWithTokensToSubgraphs(ctx, blockIDs, tokens, config)
}

func (g *GraphDB) sidToTxTs(ctx context.Context, sid []byte, config *QueryConfig) ([]*model.Tx, []*model.Transfer, error) {
	metrics := getQueryMetrics(ctx, config)
	recordMetrics := metrics != nil

	totalSize := uint64(0)
	startTime := time.Now()
	txs := make([]*model.Tx, 0, encoding.MaxTPerRecord)
	tss := make([]*model.Transfer, 0, encoding.MaxTPerRecord)
	defer func(start time.Time) {
		d := time.Since(startTime)
		if recordMetrics {
			metrics.AddTx(d, int(totalSize), len(txs))
			metrics.AddTransfer(d, int(totalSize), len(tss))
		}
	}(startTime)

	//fmt.Println(hexutil.Encode(sid))
	isETHTx := model.SIDTypeIsETHTx(sid)
	countUB := config.GetCountUpperBound()
	sizeLimit := config.GetSizeLimit()
	timeLimit := config.GetTimeout()

	start := time.Now()
	v, err := g.db.Get(sid)
	if err != nil {
		return nil, nil, err
	}
	if recordMetrics {
		metrics.AddDBGet(time.Since(start), len(v))
	}

	if isETHTx {
		t, err := encoding.DefaultEncoding.DecodeTxs(v)
		if err != nil {
			err = fmt.Errorf("decode txs sid=%s failed: %s", hexutil.Encode(sid), err.Error())
			return nil, nil, err
		}
		txs = append(txs, t...)
		totalSize += uint64(len(v))
		if totalSize > sizeLimit {
			return nil, nil, fmt.Errorf("size limit exceeded: %d > %d, total txs = %d",
				totalSize, sizeLimit, len(txs))
		}

		iter := g.db.NewIterator(sid, model.GetSIDPluralSuffix(1))
		defer iter.Release()
		for uint64(len(txs)) < countUB && iter.Next() {
			if err := iter.Error(); err != nil {
				return nil, nil, err
			}
			select {
			case <-ctx.Done():
				return nil, nil, fmt.Errorf("interrupted")
			default:
			}
			if time.Now().After(startTime.Add(timeLimit)) {
				return nil, nil, fmt.Errorf("time limit exceeded in sidsToTxTs: %s, total txs = %d, total size = %d",
					common.PrettyDuration(timeLimit).String(), len(txs), totalSize)
			}

			v := iter.Value()
			if recordMetrics {
				metrics.AddDBRangeScan(len(v))
			}
			t, err := encoding.DefaultEncoding.DecodeTxs(v)
			if err != nil {
				err = fmt.Errorf("decode txs sidp=%s failed: %s", hexutil.Encode(iter.Key()), err.Error())
				return nil, nil, err
			}
			txs = append(txs, t...)
			totalSize += uint64(len(v))
			if totalSize > sizeLimit {
				return nil, nil, fmt.Errorf("size limit exceeded: %d > %d, total txs = %d",
					totalSize, sizeLimit, len(txs))
			}
		}

		tss = nil
	} else {
		t, err := encoding.DefaultEncoding.DecodeTransfers(v)
		if err != nil {
			err = fmt.Errorf("decode tss sid=%s failed: %s", hexutil.Encode(sid), err.Error())
			return nil, nil, err
		}
		tss = append(tss, t...)
		totalSize += uint64(len(v))
		if totalSize > sizeLimit {
			return nil, nil, fmt.Errorf("size limit exceeded: %d > %d, total tss = %d",
				totalSize, sizeLimit, len(tss))
		}

		iter := g.db.NewIterator(sid, model.GetSIDPluralSuffix(1))
		defer iter.Release()
		for uint64(len(tss)) < countUB && iter.Next() {
			if err := iter.Error(); err != nil {
				return nil, nil, err
			}
			select {
			case <-ctx.Done():
				return nil, nil, fmt.Errorf("interrupted")
			default:
			}
			if time.Now().After(startTime.Add(timeLimit)) {
				return nil, nil, fmt.Errorf("time limit exceeded in sidsToTxTs: %s, total tss = %d, total size = %d",
					common.PrettyDuration(timeLimit).String(), len(tss), totalSize)
			}

			v := iter.Value()
			if recordMetrics {
				metrics.AddDBRangeScan(len(v))
			}
			t, err := encoding.DefaultEncoding.DecodeTransfers(v)
			if err != nil {
				err = fmt.Errorf("decode tss sidp=%s failed: %s", hexutil.Encode(iter.Key()), err.Error())
				return nil, nil, err
			}
			tss = append(tss, t...)
			totalSize += uint64(len(v))
			if totalSize > sizeLimit {
				return nil, nil, fmt.Errorf("size limit exceeded: %d > %d, total tss = %d",
					totalSize, sizeLimit, len(tss))
			}
		}

		txs = nil
	}

	return txs, tss, nil
}

func (g *GraphDB) sidsToTxTs(ctx context.Context, sids map[string]struct{}, config *QueryConfig) (map[string][]*model.Tx, map[string][]*model.Transfer, error) {
	metrics := getQueryMetrics(ctx, config)
	recordMetrics := metrics != nil

	totalSize := uint64(0)
	startTime := time.Now()
	txs := make(map[string][]*model.Tx)
	tss := make(map[string][]*model.Transfer)
	defer func(start time.Time) {
		d := time.Since(startTime)
		if recordMetrics {
			lenTx := 0
			for _, v := range txs {
				lenTx += len(v)
			}
			metrics.AddTx(d, int(totalSize), lenTx)
			lenTs := 0
			for _, v := range tss {
				lenTs += len(v)
			}
			metrics.AddTransfer(d, int(totalSize), lenTs)
		}
	}(startTime)

	countUB := config.GetCountUpperBound()
	sizeLimit := config.GetSizeLimit()
	timeLimit := config.GetTimeout()

	start := time.Now()
	for sid := range sids {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("interrupted")
		default:
		}
		sidBytes := []byte(sid)

		isETHTx := model.SIDTypeIsETHTx(sidBytes)
		if isETHTx {
			if _, ok := txs[sid]; ok {
				continue
			}
			v, err := g.db.Get(sidBytes)
			if err != nil && !errors.Is(err, pebble.ErrNotFound) {
				return nil, nil, err
			}
			if err != nil && errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if recordMetrics {
				metrics.AddDBGet(time.Since(start), len(v))
			}
			txs[sid] = make([]*model.Tx, 0, encoding.MaxTPerRecord)

			t, err := encoding.DefaultEncoding.DecodeTxs(v)
			if err != nil {
				err = fmt.Errorf("decode txs sid=%s failed: %s", hexutil.Encode(sidBytes), err.Error())
				return nil, nil, err
			}
			txs[sid] = append(txs[sid], t...)
			totalSize += uint64(len(v))
			if totalSize > sizeLimit {
				return nil, nil, fmt.Errorf("size limit exceeded: %d > %d, total txsid = %d, total tssid = %d",
					totalSize, sizeLimit, len(txs), len(tss))
			}

			iter := g.db.NewIterator(sidBytes, model.GetSIDPluralSuffix(1))
			defer iter.Release()
			for uint64(len(txs[sid])) < countUB && iter.Next() {
				if err := iter.Error(); err != nil {
					return nil, nil, err
				}
				select {
				case <-ctx.Done():
					return nil, nil, fmt.Errorf("interrupted")
				default:
				}
				if time.Now().After(startTime.Add(timeLimit)) {
					return nil, nil, fmt.Errorf("time limit exceeded in sidsToTxTs: %s, total txsid = %d, total tssid = %d, total size = %d",
						common.PrettyDuration(timeLimit).String(), len(txs), len(tss), totalSize)
				}

				v := iter.Value()
				if recordMetrics {
					metrics.AddDBRangeScan(len(v))
				}
				t, err := encoding.DefaultEncoding.DecodeTxs(v)
				if err != nil {
					err = fmt.Errorf("decode txs sidp=%s failed: %s", hexutil.Encode(iter.Key()), err.Error())
					return nil, nil, err
				}
				txs[sid] = append(txs[sid], t...)
				totalSize += uint64(len(v))
				if totalSize > sizeLimit {
					return nil, nil, fmt.Errorf("size limit exceeded: %d > %d, total txsid = %d, total tssid = %d",
						totalSize, sizeLimit, len(txs), len(tss))
				}
			}
		} else {
			if _, ok := tss[sid]; ok {
				continue
			}
			v, err := g.db.Get(sidBytes)
			if err != nil && !errors.Is(err, pebble.ErrNotFound) {
				return nil, nil, err
			}
			if err != nil && errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if recordMetrics {
				metrics.AddDBGet(time.Since(start), len(v))
			}
			tss[sid] = make([]*model.Transfer, 0, encoding.MaxTPerRecord)

			t, err := encoding.DefaultEncoding.DecodeTransfers(v)
			if err != nil {
				err = fmt.Errorf("decode tss sid=%s failed: %s", hexutil.Encode(sidBytes), err.Error())
				return nil, nil, err
			}
			tss[sid] = append(tss[sid], t...)
			totalSize += uint64(len(v))
			if totalSize > sizeLimit {
				return nil, nil, fmt.Errorf("size limit exceeded: %d > %d, total txsid = %d, total tssid = %d",
					totalSize, sizeLimit, len(txs), len(tss))
			}

			iter := g.db.NewIterator(sidBytes, model.GetSIDPluralSuffix(1))
			defer iter.Release()
			for uint64(len(tss[sid])) < countUB && iter.Next() {
				if err := iter.Error(); err != nil {
					return nil, nil, err
				}
				select {
				case <-ctx.Done():
					return nil, nil, fmt.Errorf("interrupted")
				default:
				}
				if time.Now().After(startTime.Add(timeLimit)) {
					return nil, nil, fmt.Errorf("time limit exceeded in sidsToTxTs: %s, total txsid = %d, total tssid = %d, total size = %d",
						common.PrettyDuration(timeLimit).String(), len(txs), len(tss), totalSize)
				}

				v := iter.Value()
				if recordMetrics {
					metrics.AddDBRangeScan(len(v))
				}
				t, err := encoding.DefaultEncoding.DecodeTransfers(v)
				if err != nil {
					err = fmt.Errorf("decode tss sidp=%s failed: %s", hexutil.Encode(iter.Key()), err.Error())
					return nil, nil, err
				}
				tss[sid] = append(tss[sid], t...)
				totalSize += uint64(len(v))
				if totalSize > sizeLimit {
					return nil, nil, fmt.Errorf("size limit exceeded: %d > %d, total txsid = %d, total tssid = %d",
						totalSize, sizeLimit, len(txs), len(tss))
				}
			}
		}
	}

	return txs, tss, nil
}

func (g *GraphDB) sidsToTxTsParallel(ctx context.Context, sids map[string]struct{}, parallel int, config *QueryConfig) (map[string][]*model.Tx, map[string][]*model.Transfer, error) {
	metrics := getQueryMetrics(ctx, config)
	recordMetrics := metrics != nil

	totalSize := uint64(0)
	startTime := time.Now()
	txs := cmap.New[[]*model.Tx]()
	tss := cmap.New[[]*model.Transfer]()
	defer func(start time.Time) {
		txsI := txs.Items()
		tssI := tss.Items()
		d := time.Since(startTime)
		if recordMetrics {
			lenTx := 0
			for _, v := range txsI {
				lenTx += len(v)
			}
			metrics.AddTx(d, int(totalSize), lenTx)
			lenTs := 0
			for _, v := range tssI {
				lenTs += len(v)
			}
			metrics.AddTransfer(d, int(totalSize), lenTs)
		}
	}(startTime)

	countUB := config.GetCountUpperBound()

	step := func(sid string) error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		default:
		}
		sidBytes := []byte(sid)

		isETHTx := model.SIDTypeIsETHTx(sidBytes)
		if isETHTx {
			start := time.Now()
			v, err := g.db.Get(sidBytes)
			if err != nil && !errors.Is(err, pebble.ErrNotFound) {
				return err
			}
			if err != nil && errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			if recordMetrics {
				metrics.AddDBGet(time.Since(start), len(v))
			}
			txs.Set(sid, nil)
			txList := make([]*model.Tx, 0, encoding.MaxTPerRecord)

			t, err := encoding.DefaultEncoding.DecodeTxs(v)
			if err != nil {
				err = fmt.Errorf("decode txs sid=%s failed: %s", hexutil.Encode(sidBytes), err.Error())
				return err
			}
			txList = append(txList, t...)
			txs.Set(sid, txList)

			iter := g.db.NewIterator(sidBytes, model.GetSIDPluralSuffix(1))
			defer iter.Release()
			for uint64(len(txList)) < countUB && iter.Next() {
				if err := iter.Error(); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return fmt.Errorf("interrupted")
				default:
				}

				v := iter.Value()
				if recordMetrics {
					metrics.AddDBRangeScan(len(v))
				}
				t, err := encoding.DefaultEncoding.DecodeTxs(v)
				if err != nil {
					err = fmt.Errorf("decode txs sidp=%s failed: %s", hexutil.Encode(iter.Key()), err.Error())
					return err
				}
				txList = append(txList, t...)
			}
			txs.Set(sid, txList)
		} else {
			start := time.Now()
			v, err := g.db.Get(sidBytes)
			if err != nil && !errors.Is(err, pebble.ErrNotFound) {
				return err
			}
			if err != nil && errors.Is(err, pebble.ErrNotFound) {
				return nil
			}
			if recordMetrics {
				metrics.AddDBGet(time.Since(start), len(v))
			}
			tss.Set(sid, nil)
			tsList := make([]*model.Transfer, 0, encoding.MaxTPerRecord)

			t, err := encoding.DefaultEncoding.DecodeTransfers(v)
			if err != nil {
				err = fmt.Errorf("decode tss sid=%s failed: %s", hexutil.Encode(sidBytes), err.Error())
				return err
			}
			tsList = append(tsList, t...)
			tss.Set(sid, tsList)

			iter := g.db.NewIterator(sidBytes, model.GetSIDPluralSuffix(1))
			defer iter.Release()
			for uint64(len(tsList)) < countUB && iter.Next() {
				if err := iter.Error(); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return fmt.Errorf("interrupted")
				default:
				}

				v := iter.Value()
				if recordMetrics {
					metrics.AddDBRangeScan(len(v))
				}
				t, err := encoding.DefaultEncoding.DecodeTransfers(v)
				if err != nil {
					err = fmt.Errorf("decode tss sidp=%s failed: %s", hexutil.Encode(iter.Key()), err.Error())
					return err
				}
				tsList = append(tsList, t...)
			}
			tss.Set(sid, tsList)
		}
		return nil
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for sid := range sids {
		s := string(sid)
		isETHTx := model.SIDTypeIsETHTx([]byte(s))
		if isETHTx && txs.Has(s) || !isETHTx && tss.Has(s) {
			continue
		}
		eg.Go(func() error {
			id := s
			return step(id)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	return txs.Items(), tss.Items(), nil
}

func (g *GraphDB) BlockIDWithTokenToTxTs(ctx context.Context, blockID uint16, token common.Address, srcID, desID uint32, getTx bool, config *QueryConfig) ([]*model.Tx, []*model.Transfer, error) {
	var txs []*model.Tx
	var tss []*model.Transfer
	var err error
	if model.TokenIsETH(token) && getTx {
		sid := model.MakeSIDWithBlockIDPack(blockID, token, srcID, desID, true)
		txs, _, err = g.sidToTxTs(ctx, sid, config)
		if err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, err
		}
		if err != nil && errors.Is(err, pebble.ErrNotFound) {
			//return nil, nil, fmt.Errorf("txs with blockID=%d, token=%s, srcID=%d, desID=%d does not exist", blockID, token.Hex(), srcID, desID)
			return nil, nil, err
		}
		tss = nil
	} else {
		sid := model.MakeSIDWithBlockIDPack(blockID, token, srcID, desID, false)
		_, tss, err = g.sidToTxTs(ctx, sid, config)
		if err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, err
		}
		if err != nil && errors.Is(err, pebble.ErrNotFound) {
			//return nil, nil, fmt.Errorf("tss with blockID=%d, token=%s, srcID=%d, desID=%d does not exist", blockID, token.Hex(), srcID, desID)
			return nil, nil, err
		}
		txs = nil
	}

	//fmt.Println(hexutil.Encode(sid))
	return txs, tss, nil
}

func (g *GraphDB) BlockIDWithTokenWithNodeIDsToTxTs(ctx context.Context, blockID uint16, token common.Address, nodeIDs [][2]uint32, getTx bool, config *QueryConfig) ([]*model.Tx, []*model.Transfer, error) {
	var txs []*model.Tx
	var tss []*model.Transfer
	var err error
	if model.TokenIsETH(token) && getTx {
		sidsSorted := make([]string, 0, len(nodeIDs))
		sids := make(map[string]struct{}, len(nodeIDs))
		for _, nodeID := range nodeIDs {
			sid := model.MakeSIDWithBlockIDPack(blockID, token, nodeID[0], nodeID[1], true)
			sidsSorted = append(sidsSorted, string(sid))
			sids[string(sid)] = struct{}{}
		}
		fetchThreads := config.GetFetchThreads()
		var txMap map[string][]*model.Tx
		if fetchThreads == 1 {
			txMap, _, err = g.sidsToTxTs(ctx, sids, config)
		} else {
			txMap, _, err = g.sidsToTxTsParallel(ctx, sids, int(fetchThreads), config)
		}
		if err != nil {
			return nil, nil, err
		}
		txs = make([]*model.Tx, 0, len(txMap)*8)
		for _, sid := range sidsSorted {
			txs = append(txs, txMap[sid]...)
		}
		tss = nil
	} else {
		sidsSorted := make([]string, 0, len(nodeIDs))
		sids := make(map[string]struct{}, len(nodeIDs))
		for _, nodeID := range nodeIDs {
			sid := model.MakeSIDWithBlockIDPack(blockID, token, nodeID[0], nodeID[1], false)
			sidsSorted = append(sidsSorted, string(sid))
			sids[string(sid)] = struct{}{}
		}
		fetchThreads := config.GetFetchThreads()
		var tsMap map[string][]*model.Transfer
		if fetchThreads == 1 {
			_, tsMap, err = g.sidsToTxTs(ctx, sids, config)
		} else {
			_, tsMap, err = g.sidsToTxTsParallel(ctx, sids, int(fetchThreads), config)
		}
		if err != nil {
			return nil, nil, err
		}
		tss = make([]*model.Transfer, 0, len(tsMap)*8)
		for _, sid := range sidsSorted {
			tss = append(tss, tsMap[sid]...)
		}
		txs = nil
	}
	return txs, tss, nil
}

func QueryMGEdgesParallel(g *GraphDB, mg search.MainGraph, mergedSubgraph *model.Subgraph, mergedRMap []string, originSubgraphs []*model.Subgraph, parallel int, ctx context.Context, qconfig *QueryConfig) ([]*model.Tx, []*model.Transfer, error) {
	if mergedRMap == nil {
		mergedRMap = model.ReverseAddressMap(mergedSubgraph.AddressMap)
	}
	var ETHSubgraph *model.Subgraph = nil
	tokenSubgraphs := make([]*model.Subgraph, 0, len(originSubgraphs))
	for _, originSubgraph := range originSubgraphs {
		if originSubgraph.Token.Cmp(model.EtherAddress) == 0 {
			ETHSubgraph = originSubgraph
		} else {
			tokenSubgraphs = append(tokenSubgraphs, originSubgraph)
		}
	}
	var retTxs []*model.Tx
	retTss := struct {
		sync.Mutex
		tss []*model.Transfer
	}{
		tss: make([]*model.Transfer, 0, len(mg)*8),
	}
	iterETHSubgraph := func() error {
		srcDesPairs := make([][2]uint32, 0, len(mg)*8)
		for src, desMap := range mg {
			srcID, ok := ETHSubgraph.AddressMap[mergedRMap[src]]
			if !ok {
				continue
			}
			for des := range desMap {
				desID, ok := ETHSubgraph.AddressMap[mergedRMap[des]]
				if !ok {
					continue
				}
				if ETHSubgraph.IsLinked(srcID, desID) {
					srcDesPairs = append(srcDesPairs, [2]uint32{srcID, desID})
				}
			}
		}
		txs, _, err := g.BlockIDWithTokenWithNodeIDsToTxTs(ctx, ETHSubgraph.BlockID, ETHSubgraph.Token, srcDesPairs, true, qconfig)
		if err != nil {
			return err
		}
		retTxs = txs
		_, tss, err := g.BlockIDWithTokenWithNodeIDsToTxTs(ctx, ETHSubgraph.BlockID, ETHSubgraph.Token, srcDesPairs, false, qconfig)
		if err != nil {
			return err
		}
		retTss.Lock()
		retTss.tss = append(retTss.tss, tss...)
		retTss.Unlock()
		return nil
	}
	iterTokenSubgraph := func(i int) error {
		tokenSubgraph := tokenSubgraphs[i]
		srcDesPairs := make([][2]uint32, 0, len(mg)*8)
		for src, desMap := range mg {
			srcID, ok := tokenSubgraph.AddressMap[mergedRMap[src]]
			if !ok {
				continue
			}
			for des := range desMap {
				desID, ok := tokenSubgraph.AddressMap[mergedRMap[des]]
				if !ok {
					continue
				}
				if tokenSubgraph.IsLinked(srcID, desID) {
					srcDesPairs = append(srcDesPairs, [2]uint32{srcID, desID})
				}
			}
		}
		_, tss, err := g.BlockIDWithTokenWithNodeIDsToTxTs(ctx, tokenSubgraph.BlockID, tokenSubgraph.Token, srcDesPairs, false, qconfig)
		if err != nil {
			return err
		}
		retTss.Lock()
		retTss.tss = append(retTss.tss, tss...)
		retTss.Unlock()
		return nil
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	if ETHSubgraph != nil {
		eg.Go(func() error {
			return iterETHSubgraph()
		})
	}
	for i := range tokenSubgraphs {
		s := i
		eg.Go(func() error {
			return iterTokenSubgraph(s)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	return retTxs, retTss.tss, nil
}

func QueryMGEdgesParallelBatch(g *GraphDB, mgs []search.MainGraph, mergedSubgraphs []*model.Subgraph, mergedRMaps [][]string, originSubgraphss [][]*model.Subgraph, parallel int, ctx context.Context, qconfig *QueryConfig) ([]*model.Tx, []*model.Transfer, error) {
	txs := make([]*model.Tx, 0)
	tss := make([]*model.Transfer, 0)
	for i := 0; i < len(mergedSubgraphs); i++ {
		txsi, tssi, err := QueryMGEdgesParallel(g, mgs[i], mergedSubgraphs[i], mergedRMaps[i], originSubgraphss[i], 4, context.Background(), qconfig)
		if err != nil {
			return nil, nil, err
		}
		txs = append(txs, txsi...)
		tss = append(tss, tssi...)
	}
	return txs, tss, nil
}

func (g *GraphDB) LatestBlockID() uint16 {
	return 0
	snap, err := g.db.NewSnapshot()
	if err != nil {
		log.Error("read LatestBlock: create snapshot failed", "err", err.Error())
		return 0
	}
	defer snap.Release()

	iter := g.db.NewIterator(model.SubgraphPrefix, nil)
	defer iter.Release()
	return 0
	//return binary.BigEndian.Uint16(iter.Key()[len(model.SubgraphPrefix) : len(model.SubgraphPrefix)+2])
}
