package pricedb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
	"transfer-graph/opensearch"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/fdlimit"
	"github.com/ethereum/go-ethereum/common/hexutil"
	cmap "github.com/orcaman/concurrent-map/v2"
	"golang.org/x/sync/errgroup"
)

const (
	reservedFds      = 4096
	ParallelPoolSize = 1024
)

type PriceDB struct {
	sync.Mutex

	db *Database

	parallelLimit uint64
}

func (p *PriceDB) Close() {
	p.db.Close()
}

func NewPriceDB(datadir string, readonly bool) (*PriceDB, error) {
	maxFd, err := fdlimit.Maximum()
	if err != nil {
		return nil, err
	}
	maxFd -= reservedFds
	cache := 2048
	db, err := New(path.Join(datadir, "db"), cache, maxFd, "", false, false)
	if err != nil {
		return nil, err
	}
	p := &PriceDB{
		db:            db,
		parallelLimit: uint64(ParallelPoolSize),
	}
	return p, nil
}

type WriteRecord struct {
	BlockID uint16
	Token   common.Address
	Price   float64
}

type WriteRequest struct {
	Desc     string
	Contents []*WriteRecord
	Parallel int
}

func (p *PriceDB) Write(req *WriteRequest, ctx context.Context) error {
	p.Lock()
	defer p.Unlock()

	if len(req.Contents) == 0 {
		return fmt.Errorf("empty request: %s", req.Desc)
	}
	wrapError := func(msg string, e error) error {
		return fmt.Errorf("%s: %s", msg, e)
	}

	batch := p.db.NewBatch()
	fmt.Printf("[Debug] {Write} batch put start, req.Cotents: %d\n", len(req.Contents))

	for _, record := range req.Contents {
		if batch.ValueSize() > math.MaxUint32*3/4 {
			fmt.Println("[Debug] {Write} batch > 3GB, inter write start")
			if err := batch.Write(); err != nil {
				fmt.Println(err)
				return wrapError("batch write", err)
			}
			fmt.Println("[Debug] {SWrite} inter swrite finished")
			batch.Reset()
		}

		v := EncodePrice(record.Price)
		pid := MakePIDWithBlockID(record.BlockID, record.Token)
		if err := batch.Put(pid, v); err != nil {
			return wrapError("put price", err)
		}
	}
	if err := batch.Write(); err != nil {
		return wrapError("batch write", err)
	}
	return nil
}

func (p *PriceDB) WriteZ(req *WriteRequest, ctx context.Context) error {
	p.Lock()
	defer p.Unlock()

	if len(req.Contents) == 0 {
		return fmt.Errorf("empty request: %s", req.Desc)
	}
	wrapError := func(msg string, e error) error {
		return fmt.Errorf("%s: %s", msg, e)
	}

	readReq := &ReadRequest{
		Contents: make([]*ReadRecord, len(req.Contents)),
		Parallel: req.Parallel,
	}
	for i, record := range req.Contents {
		readReq.Contents[i] = &ReadRecord{
			BlockID: record.BlockID,
			Token:   record.Token,
		}
	}
	if err := p.Read(readReq, ctx); err != nil {
		return wrapError("read price", err)
	}

	batch := p.db.NewBatch()
	fmt.Printf("[Debug] {Write} batch put start, req.Cotents: %d\n", len(req.Contents))

	for i, record := range req.Contents {
		if readReq.Contents[i].Price != 0 {
			continue
		}

		if batch.ValueSize() > math.MaxUint32*3/4 {
			fmt.Println("[Debug] {Write} batch > 3GB, inter write start")
			if err := batch.Write(); err != nil {
				fmt.Println(err)
				return wrapError("batch write", err)
			}
			fmt.Println("[Debug] {SWrite} inter swrite finished")
			batch.Reset()
		}

		v := EncodePrice(record.Price)
		pid := MakePIDWithBlockID(record.BlockID, record.Token)
		if err := batch.Put(pid, v); err != nil {
			return wrapError("put price", err)
		}
	}
	if err := batch.Write(); err != nil {
		return wrapError("batch write", err)
	}
	return nil
}

func (p *PriceDB) pidsToPricesParallel(pids map[string]struct{}, parallel int, ctx context.Context) (map[string]float64, error) {
	ret := cmap.New[float64]()
	step := func(pid string) error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted")
		default:
		}
		ret.Set(pid, 0)
		v, err := p.db.Get([]byte(pid))
		if err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return err
		}
		if err != nil && errors.Is(err, pebble.ErrNotFound) {
			return nil
		}
		t, err := DecodePrice(v)
		if err != nil {
			err = fmt.Errorf("decode gid=%s failed: %s", hexutil.Encode([]byte(pid)), err.Error())
			return err
		}
		ret.Set(pid, t)
		return nil
	}
	eg := errgroup.Group{}
	eg.SetLimit(parallel)
	for pid := range pids {
		s := string(pid)
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

func (p *PriceDB) pidsToPrices(pids map[string]struct{}, ctx context.Context) (map[string]float64, error) {
	ret := make(map[string]float64, len(pids))
	for pid := range pids {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("interrupted")
		default:
		}
		ret[pid] = 0
		v, err := p.db.Get([]byte(pid))
		if err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}
		if err != nil && errors.Is(err, pebble.ErrNotFound) {
			ret[pid] = 0
			continue
		}
		t, err := DecodePrice(v)
		if err != nil {
			err = fmt.Errorf("decode gid=%s failed: %s", hexutil.Encode([]byte(pid)), err.Error())
			return nil, err
		}
		ret[pid] = t
	}
	return ret, nil
}

type ReadRecord struct {
	BlockID uint16
	Token   common.Address
	Price   float64
}

type ReadRequest struct {
	Desc     string
	Contents []*ReadRecord
	Parallel int
}

func (p *PriceDB) Read(req *ReadRequest, ctx context.Context) error {
	pids := make(map[string]struct{}, len(req.Contents))
	pidsSorted := make([]string, 0, len(req.Contents))
	for _, record := range req.Contents {
		pid := MakePIDWithBlockID(record.BlockID, record.Token)
		pids[string(pid)] = struct{}{}
		pidsSorted = append(pidsSorted, string(pid))
	}
	var m map[string]float64 = nil
	var err error
	if req.Parallel == 1 {
		m, err = p.pidsToPrices(pids, ctx)
	} else {
		m, err = p.pidsToPricesParallel(pids, req.Parallel, ctx)
	}
	if err != nil {
		return err
	}
	for i := range pidsSorted {
		req.Contents[i].Price = m[pidsSorted[i]]
	}
	return nil
}

func (p *PriceDB) TokensWithBlocks(tokens []common.Address, blocks []uint64, parallel int, ctx context.Context) ([]float64, error) {
	if len(tokens) != len(blocks) {
		return nil, fmt.Errorf("mis-matched tokens[] and blocks[]")
	}
	/*
		readReq := &ReadRequest{
			Contents: make([]*ReadRecord, 0, len(tokens)),
			Parallel: parallel,
		}
		for i := range tokens {
			readReq.Contents = append(readReq.Contents, &ReadRecord{
				BlockID: GetBlockID(blocks[i]),
				Token:   tokens[i],
			})
		}
		if err := p.Read(readReq, ctx); err != nil {
			return nil, err
		}
		ret := make([]float64, 0, len(tokens))
		for _, record := range readReq.Contents {
			ret = append(ret, record.Price)
		}
	*/
	pids := make(map[string]struct{}, len(tokens))
	pidsSorted := make([]string, 0, len(tokens))
	for i := range tokens {
		pid := MakePIDWithBlockID(GetBlockID(blocks[i]), tokens[i])
		pids[string(pid)] = struct{}{}
		pidsSorted = append(pidsSorted, string(pid))
	}
	var m map[string]float64 = nil
	var err error
	if parallel == 1 {
		m, err = p.pidsToPrices(pids, ctx)
	} else {
		m, err = p.pidsToPricesParallel(pids, parallel, ctx)
	}
	if err != nil {
		return nil, err
	}
	ret := make([]float64, len(tokens))
	for i := range pidsSorted {
		ret[i] = m[pidsSorted[i]]
	}
	return ret, nil
}

func SyncByOpenSearch(p *PriceDB, sBlock, eBlock uint64, tokenList []common.Address, parallel int, ctx context.Context, config *opensearch.OpenSearchConfig) error {
	type btime struct {
		block     uint64
		timestamp uint64
	}
	timeLayout := "2006-01-02T15:04:05Z"
	sBlock -= sBlock % BlockSpan
	eBlock -= eBlock % BlockSpan

	btimes := make([]btime, 0, (eBlock-sBlock)/BlockSpan)
	for b := sBlock; b < eBlock; b += BlockSpan {
		tb := b
		qres, err := opensearch.QueryOpenSearch(b, b+1, 1, config)
		for err != nil || len(qres) == 0 || len(qres[0].Txs) == 0 || len(qres[0].Transfers) == 0 {
			fmt.Println("[Debug] opensearch block empty:", b)
			if BlockSpan/100 > 1 {
				tb += BlockSpan / 100
			} else {
				tb += 1
			}
			if tb >= b+BlockSpan {
				break
			}
			qres, err = opensearch.QueryOpenSearch(tb, tb+1, 1, config)
		}
		//r := rand.New(rand.NewSource(time.Now().UnixNano()))
		t, err := time.Parse(timeLayout, qres[0].Txs[0].Time)
		if err != nil {
			return err
		}
		btimes = append(btimes, btime{
			block:     tb,
			timestamp: uint64(t.Unix()),
		})
		//fmt.Println("[Debug] opensearch block done:", b)
	}

	writeReq := &WriteRequest{
		Desc:     "SyncByOpenSearch",
		Contents: make([]*WriteRecord, 0, len(btimes)*len(tokenList)),
		Parallel: parallel,
	}
	for i := range btimes {
		fmt.Println("[Debug] FetchPrice block start:", btimes[i].block)
		//prices, err := FetchPrice(btimes[i].block, btimes[i].timestamp, tokenList)
		//prices, err := FetchPriceMultiReq(btimes[i].block, btimes[i].timestamp, tokenList, 10, time.Duration(100)*time.Millisecond)
		prices, err := FetchPriceRetry(btimes[i].block, btimes[i].timestamp, tokenList, 5, time.Duration(100)*time.Millisecond)
		if err != nil {
			return err
		}
		for j, token := range tokenList {
			writeReq.Contents = append(writeReq.Contents, &WriteRecord{
				BlockID: uint16(btimes[i].block / BlockSpan),
				Token:   token,
				Price:   prices[j],
			})
		}
		fmt.Println("[Debug] FetchPrice block done:", btimes[i].block)
	}
	if err := p.WriteZ(writeReq, ctx); err != nil {
		return err
	}
	return nil
}

func (p *PriceDB) SimpleWriteDecimals(tokens []common.Address, decimals []uint8) error {
	p.Lock()
	defer p.Unlock()

	if len(tokens) != len(decimals) || len(tokens) == 0 {
		return fmt.Errorf("request size error")
	}
	wrapError := func(msg string, e error) error {
		return fmt.Errorf("%s: %s", msg, e)
	}

	batch := p.db.NewBatch()
	for i := range tokens {
		v := EncodeDecimal(decimals[i])
		did := MakeDID(tokens[i])
		if err := batch.Put(did, v); err != nil {
			return wrapError("put decimal", err)
		}
	}

	if err := batch.Write(); err != nil {
		return wrapError("batch write", err)
	}
	return nil
}

func (p *PriceDB) SimpleReadDecimals(tokens []common.Address) ([]uint8, map[string]uint8, error) {
	decimals := make([]uint8, len(tokens))
	decimalsMap := make(map[string]uint8, len(tokens))
	for i, token := range tokens {
		did := MakeDID(token)
		v, err := p.db.Get(did)
		if err != nil {
			return nil, nil, err
		}
		decimals[i], err = DecodeDecimal(v)
		if err != nil {
			return nil, nil, err
		}
		decimalsMap[string(token.Bytes())] = decimals[i]
	}
	return decimals, decimalsMap, nil
}

// NOTE: if to use this function, set NeedDecimalPrefix=true while setting up the db.
func (p *PriceDB) SimpleReadAllDecimals() (map[string]uint8, error) {
	if !NeedDecimalPrefix {
		return nil, fmt.Errorf("'NeedDecimalPrefix'=false")
	}
	decimals := make(map[string]uint8)
	iter := p.db.NewIterator(DecimalPrefix, nil)
	defer iter.Release()
	for iter.Next() {
		var err error
		if err = iter.Error(); err != nil {
			return nil, err
		}
		kShallow := iter.Key()
		k := make([]byte, len(kShallow))
		copy(k, kShallow)
		v := iter.Value()
		decimals[ExtractTokenFromDIDAsString(k)], err = DecodeDecimal(v)
		if err != nil {
			return nil, err
		}
	}
	return decimals, nil
}

// sync by file formatted as r"([addr],[decimals]\n)*[addr],[decimals]"
func SimpleSyncDecimals(p *PriceDB, dataDir, fileName string) error {
	file, err := os.ReadFile(path.Join(dataDir, fileName))
	if err != nil {
		return err
	}
	records := strings.Split(string(file), "\n")
	tokens := make([]common.Address, 0, len(records))
	decimalss := make([]uint8, 0, len(records))
	for _, record := range records {
		items := strings.Split(record, ",")
		decimals, err := strconv.Atoi(items[1])
		if err != nil || len(items[0]) != 20*2+2 || decimals < 0 || decimals > math.MaxUint8 {
			continue
		}
		tokens = append(tokens, common.HexToAddress(items[0]))
		decimalss = append(decimalss, uint8(decimals))
	}
	if err := p.SimpleWriteDecimals(tokens, decimalss); err != nil {
		return err
	}
	return nil
}
