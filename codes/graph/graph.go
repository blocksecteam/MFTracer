package graph

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"path"
	"sync"
	"time"
	"transfer-graph/encoding"
	"transfer-graph/model"

	"github.com/cockroachdb/pebble"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/fdlimit"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
)

const SubgraphWRDescPrefix = "SUBGRAPH WRITE: "

var reservedFds = 4096
var WriteMetadata = false

type MetricsKey string

var (
	ParallelPoolSize          = 1024
	DefaultWriteRecordHistory = 1048576
	QueryMetricsKey           = MetricsKey("QUERY_METRICS")
	WriteMetricsKey           = MetricsKey("WRITE_METRICS")
)

func getWriteMetrics(ctx context.Context) *WriteMetrics {
	if v := ctx.Value(WriteMetricsKey); v != nil {
		m := v.(*WriteMetrics)
		return m
	}
	return nil
}

type GraphDB struct {
	sync.Mutex

	// multiplexer
	db     *Database
	metaDB *Database

	parallelLimit uint64
}

func (g *GraphDB) Close() {
	g.db.Close()
	g.metaDB.Close()
}

type WriteRecord struct {
	StartTime        time.Time `json:"startTime"`
	EndTime          time.Time `json:"endTime"`
	Desc             string    `json:"desc"`
	EdgeCount        uint64    `json:"edgeCount"`
	AddressCount     uint64    `json:"addressCount"`
	TxCount          uint64    `json:"txCount"`
	IndexCount       uint64    `json:"indexCount"`
	BytesWritten     uint64    `json:"bytesWritten"`
	MinBlock         uint64    `json:"minBlock"`
	MaxBlock         uint64    `json:"maxBlock"`
	TxMetaCount      uint64    `json:"txMetaCount"`
	AddressMetaCount uint64    `json:"addrMetaCount"`
	SIDCount         uint64    `json:"sidCount"`
	GIDCount         uint64    `json:"gidCount"`

	addressMap map[common.Address]struct{} `json:"-"`
}

func newWriteRecord(desc string) *WriteRecord {
	return &WriteRecord{
		StartTime:  time.Now(),
		Desc:       desc,
		MinBlock:   math.MaxUint64,
		addressMap: make(map[common.Address]struct{}),
	}
}

func (w *WriteRecord) AddTx(s int, l int) {
	w.TxCount += uint64(l)
	w.BytesWritten += uint64(s)
}

func (w *WriteRecord) AddIndex(s int) {
	w.IndexCount += 1
	w.BytesWritten += uint64(s)
}

func (w *WriteRecord) AddTransfer(s int, l int) {
	w.EdgeCount += uint64(l)
	w.BytesWritten += uint64(s)
}

func (w *WriteRecord) AddSubgraph(g *model.Subgraph, s int) {
	w.EdgeCount += uint64(len(g.Columns))
	w.AddressCount += uint64(len(g.NodePtrs) - 1)
	w.BytesWritten += uint64(s)
	w.MinBlock = uint64(g.BlockID)
	w.MaxBlock = uint64(g.BlockID)
	w.GIDCount += 1
}

func (w *WriteRecord) AddSID() {
	w.SIDCount += 1
}

type DBMetadata struct {
	EarliestBlock     uint64        `json:"earliestBlock"`
	LatestBlock       uint64        `json:"latestBlock"`
	EdgeCount         *big.Int      `json:"edgeCount"`
	WriteRecordsLimit uint64        `json:"writeRecordsLimit"`
	WriteRecords      []WriteRecord `json:"writeRecords"`
}

func newDBMetadata() *DBMetadata {
	return &DBMetadata{WriteRecordsLimit: uint64(DefaultWriteRecordHistory)}
}

func (m *DBMetadata) AppendWriteRecords(w *WriteRecord) {
	l := uint64(len(m.WriteRecords))
	if l > m.WriteRecordsLimit {
		m.WriteRecords = m.WriteRecords[l-m.WriteRecordsLimit:]
	}
	m.WriteRecords = append(m.WriteRecords, *w)
}

func (m *DBMetadata) Write(b ethdb.Batch) (int, error) {
	raw, err := json.Marshal(m)
	if err != nil {
		return 0, err
	}
	if err := b.Put(model.MetadataKey, raw); err != nil {
		return 0, err
	}
	return len(raw), nil
}

func NewGraphDB(datadir string, readonly bool) (*GraphDB, error) {
	startTime := time.Now()

	maxFd, err := fdlimit.Maximum()
	if err != nil {
		return nil, err
	}
	maxFd -= reservedFds

	fd := maxFd
	cache := 2048
	db, err := New(path.Join(datadir, "db"), cache, fd, "", false, false)
	if err != nil {
		return nil, fmt.Errorf("NewGraphDB: %s", err.Error())
	}

	metaCache := 128
	metaFd := 128
	metaDB, err := New(path.Join(datadir, "metadata"), metaCache, metaFd, "", false, false)
	if err != nil {
		return nil, fmt.Errorf("NewGraphDB: %s", err.Error())
	}

	g := &GraphDB{
		db:     db,
		metaDB: metaDB,

		parallelLimit: uint64(ParallelPoolSize),
	}
	log.Info("load graph db finished", "datadir", datadir, "duration", time.Since(startTime), "latestBlock", g.LatestBlockID())
	return g, nil
}

func (g *GraphDB) GetMetadata() (*DBMetadata, error) {
	v, err := g.metaDB.Get(model.MetadataKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return newDBMetadata(), nil
	}
	if err != nil {
		return nil, err
	}

	m := &DBMetadata{}
	if err := json.Unmarshal(v, m); err != nil {
		return nil, err
	}
	return m, nil
}

type SRecord struct {
	Token     common.Address    `json:"token"`
	SrcID     uint32            `json:"srcID"`
	DesID     uint32            `json:"desID"`
	Transfers []*model.Transfer `json:"transfers"`
	Txs       []*model.Tx       `json:"txs"`
}

type SWriteRequest struct {
	Desc     string
	BlockID  uint16     `json:"blockID"`
	Contents []*SRecord `json:"contents"`
}

type CompositeGRecord struct {
	Subgraph *model.Subgraph  `json:"subgraph"`
	Tokens   []common.Address `json:"tokens"`
}

type GWriteRequest struct {
	Desc              string
	Contents          []*model.Subgraph            `json:"contents"`
	CompositeContents map[string]*CompositeGRecord `json:"compositeContents"`
}

/*
func (g *GraphDB) SWrite(ctx context.Context, req *SWriteRequest) error {
	g.Lock()
	defer g.Unlock()

	if len(req.Contents) == 0 && len(req.Contents) == 0 {
		return fmt.Errorf("empty request: %s", req.Desc)
	}
	wrapError := func(msg string, e error) error {
		return fmt.Errorf("%s: %s", msg, e)
	}

	m := getWriteMetrics(ctx)
	metadata, err := g.GetMetadata()
	if err != nil {
		return wrapError("cannot load metadata", err)
	}
	writeRecord := newWriteRecord(req.Desc)

	batch := g.db.NewBatch()
	isETH := (req.Token.Cmp(model.EtherAddress) == 0)
	if isETH {
		for _, record := range req.Contents {
			if record.Txs != nil {
				values, err := encoding.DefaultEncoding.EncodeTxs(record.Txs)
				if err != nil {
					return wrapError("encode ETH txs", err)
				}
				vLen := len(values)
				sid := model.MakeETHSIDWithBlockID(req.BlockID, true, record.SrcID, record.DesID)
				if err := batch.Put(sid, values[0]); err != nil {
					return wrapError("put txs content", err)
				}
				txAmount := int(binary.BigEndian.Uint16(values[0][:2]))
				writeRecord.AddTx(len(values[0])+len(sid), txAmount)
				if m != nil {
					m.AddTx(len(values[0])+len(sid), txAmount)
				}
				for i := 1; i < vLen; i++ {
					sidp := model.MakeSIDPlural(sid, uint16(i))
					if err := batch.Put(sidp, values[i]); err != nil {
						return wrapError("put txs content", err)
					}
					txAmount := int(binary.BigEndian.Uint16(values[i][:2]))
					writeRecord.AddTx(len(values[i])+len(sidp), txAmount)
					if m != nil {
						m.AddTx(len(values[i])+len(sid), txAmount)
					}
				}
			}
			if record.Transfers != nil {
				values, err := encoding.DefaultEncoding.EncodeTransfers(record.Transfers)
				if err != nil {
					return wrapError("encode ETH tsfs", err)
				}
				vLen := len(values)
				sid := model.MakeETHSIDWithBlockID(req.BlockID, false, record.SrcID, record.DesID)
				if err := batch.Put(sid, values[0]); err != nil {
					return wrapError("put tss content", err)
				}
				tsAmount := int(binary.BigEndian.Uint16(values[0][:2]))
				writeRecord.AddTransfer(len(values[0])+len(sid), tsAmount)
				if m != nil {
					m.AddTransfer(len(values[0])+len(sid), tsAmount)
				}
				for i := 1; i < vLen; i++ {
					sidp := model.MakeSIDPlural(sid, uint16(i))
					if err := batch.Put(sidp, values[i]); err != nil {
						return wrapError("put tss content", err)
					}
					tsAmount := int(binary.BigEndian.Uint16(values[i][:2]))
					writeRecord.AddTransfer(len(values[i])+len(sidp), tsAmount)
					if m != nil {
						m.AddTransfer(len(values[i])+len(sid), tsAmount)
					}
				}
			}
		}
	} else {
		for _, record := range req.Contents {
			values, err := encoding.DefaultEncoding.EncodeTransfers(record.Transfers)
			if err != nil {
				return wrapError("encode token tsfs", err)
			}
			vLen := len(values)
			sid := model.MakeSIDWithBlockID(req.BlockID, req.Token, record.SrcID, record.DesID)
			if err := batch.Put(sid, values[0]); err != nil {
				return wrapError("put tss content", err)
			}
			tsAmount := int(binary.BigEndian.Uint16(values[0][:2]))
			writeRecord.AddTransfer(len(values[0])+len(sid), tsAmount)
			if m != nil {
				m.AddTransfer(len(values[0])+len(sid), tsAmount)
			}
			for i := 1; i < vLen; i++ {
				sidp := model.MakeSIDPlural(sid, uint16(i))
				if err := batch.Put(sidp, values[i]); err != nil {
					return wrapError("put txs content", err)
				}
				tsAmount := int(binary.BigEndian.Uint16(values[i][:2]))
				writeRecord.AddTransfer(len(values[i])+len(sidp), tsAmount)
				if m != nil {
					m.AddTransfer(len(values[i])+len(sid), tsAmount)
				}
			}
		}
	}

	if WriteMetadata {
		metaBatch := g.metaDB.NewBatch()
		writeRecord.EndTime = time.Now()
		metadata.AppendWriteRecords(writeRecord)
		_, err := metadata.Write(metaBatch)
		if err != nil {
			return wrapError("write metadata", err)
		}
		if err := metaBatch.Write(); err != nil {
			return wrapError("metaDB batch write", err)
		}
	}

	if err := batch.Write(); err != nil {
		return wrapError("batch write", err)
	}
	return nil
}
*/

func (g *GraphDB) SWrite(ctx context.Context, req *SWriteRequest) error {
	g.Lock()
	defer g.Unlock()

	if len(req.Contents) == 0 { //len(req.Contents) == 0 && len(req.Contents) == 0 {
		return fmt.Errorf("empty request: %s", req.Desc)
	}
	wrapError := func(msg string, e error) error {
		return fmt.Errorf("%s: %s", msg, e)
	}

	m := getWriteMetrics(ctx)
	metadata, err := g.GetMetadata()
	if err != nil {
		return wrapError("cannot load metadata", err)
	}
	writeRecord := newWriteRecord(req.Desc)

	batch := g.db.NewBatch()
	fmt.Printf("[Debug] {SWrite} batch put start, req.Cotents: %d\n", len(req.Contents))

	for _, record := range req.Contents {

		if batch.ValueSize() > math.MaxUint32*3/4 {
			fmt.Println("[Debug] {SWrite} batch > 3GB, inter swrite start")
			if err := batch.Write(); err != nil {
				fmt.Println(err)
				return wrapError("batch write", err)
			}
			fmt.Println("[Debug] {SWrite} inter swrite finished")
			batch.Reset()
		}

		isETH := (record.Token.Cmp(model.EtherAddress) == 0)
		if isETH {
			if record.Txs != nil {
				values, err := encoding.DefaultEncoding.EncodeTxs(record.Txs)
				if err != nil {
					return wrapError("encode ETH txs", err)
				}
				vLen := len(values)
				sid := model.MakeETHSIDWithBlockID(req.BlockID, true, record.SrcID, record.DesID)
				if err := batch.Put(sid, values[0]); err != nil {
					return wrapError("put txs content", err)
				}
				txAmount := int(binary.BigEndian.Uint16(values[0][:2]))
				writeRecord.AddTx(len(values[0])+len(sid), txAmount)
				if m != nil {
					m.AddTx(len(values[0])+len(sid), txAmount)
				}
				for i := 1; i < vLen; i++ {
					sidp := model.MakeSIDPlural(sid, uint16(i))
					if err := batch.Put(sidp, values[i]); err != nil {
						return wrapError("put txs content", err)
					}
					txAmount := int(binary.BigEndian.Uint16(values[i][:2]))
					writeRecord.AddTx(len(values[i])+len(sidp), txAmount)
					if m != nil {
						m.AddTx(len(values[i])+len(sidp), txAmount)
					}
				}
			}
			if record.Transfers != nil {
				values, err := encoding.DefaultEncoding.EncodeTransfers(record.Transfers)
				if err != nil {
					return wrapError("encode ETH tsfs", err)
				}
				vLen := len(values)
				sid := model.MakeETHSIDWithBlockID(req.BlockID, false, record.SrcID, record.DesID)
				if err := batch.Put(sid, values[0]); err != nil {
					return wrapError("put tss content", err)
				}
				tsAmount := int(binary.BigEndian.Uint16(values[0][:2]))
				writeRecord.AddTransfer(len(values[0])+len(sid), tsAmount)
				if m != nil {
					m.AddTransfer(len(values[0])+len(sid), tsAmount)
				}
				for i := 1; i < vLen; i++ {
					sidp := model.MakeSIDPlural(sid, uint16(i))
					if err := batch.Put(sidp, values[i]); err != nil {
						return wrapError("put tss content", err)
					}
					tsAmount := int(binary.BigEndian.Uint16(values[i][:2]))
					writeRecord.AddTransfer(len(values[i])+len(sidp), tsAmount)
					if m != nil {
						m.AddTransfer(len(values[i])+len(sidp), tsAmount)
					}
				}
			}
		} else {
			values, err := encoding.DefaultEncoding.EncodeTransfers(record.Transfers)
			if err != nil {
				return wrapError("encode token tsfs", err)
			}
			vLen := len(values)
			sid := model.MakeSIDWithBlockID(req.BlockID, record.Token, record.SrcID, record.DesID)
			if err := batch.Put(sid, values[0]); err != nil {
				return wrapError("put tss content", err)
			}
			tsAmount := int(binary.BigEndian.Uint16(values[0][:2]))
			writeRecord.AddTransfer(len(values[0])+len(sid), tsAmount)
			if m != nil {
				m.AddTransfer(len(values[0])+len(sid), tsAmount)
			}
			for i := 1; i < vLen; i++ {
				sidp := model.MakeSIDPlural(sid, uint16(i))
				if err := batch.Put(sidp, values[i]); err != nil {
					return wrapError("put txs content", err)
				}
				tsAmount := int(binary.BigEndian.Uint16(values[i][:2]))
				writeRecord.AddTransfer(len(values[i])+len(sidp), tsAmount)
				if m != nil {
					m.AddTransfer(len(values[i])+len(sidp), tsAmount)
				}
			}
		}
	}

	if WriteMetadata {
		metaBatch := g.metaDB.NewBatch()
		writeRecord.EndTime = time.Now()
		metadata.AppendWriteRecords(writeRecord)
		_, err := metadata.Write(metaBatch)
		if err != nil {
			return wrapError("write metadata", err)
		}
		if err := metaBatch.Write(); err != nil {
			return wrapError("metaDB batch write", err)
		}
	}
	fmt.Println("[Debug] {SWrite} swrite start")
	if err := batch.Write(); err != nil {
		fmt.Println(err)
		return wrapError("batch write", err)
	}
	fmt.Println("[Debug] {SWrite} swrite finished")
	return nil
}

func (g *GraphDB) GWrite(ctx context.Context, req *GWriteRequest) error {
	g.Lock()
	defer g.Unlock()

	if req.Contents == nil {
		return fmt.Errorf("empty request: %s", req.Desc)
	}
	wrapError := func(msg string, e error) error {
		return fmt.Errorf("%s: %s", msg, e)
	}

	m := getWriteMetrics(ctx)
	metadata, err := g.GetMetadata()
	if err != nil {
		return wrapError("cannot load metadata", err)
	}
	writeRecord := newWriteRecord(req.Desc)

	batch := g.db.NewBatch()
	for _, subgraph := range req.Contents {
		v, err := encoding.DefaultEncoding.EncodeSubgraph(subgraph)
		if err != nil {
			return wrapError("encode subgraph", err)
		}
		gid := model.MakeGIDWithBlockIDPack(subgraph.BlockID, subgraph.Token)
		if err := batch.Put(gid, v); err != nil {
			return wrapError("put subgraph", err)
		}
		writeRecord.AddSubgraph(subgraph, len(v)+len(gid))
		if m != nil {
			m.AddSubgraph(len(v) + len(gid))
		}
	}

	for gid, csubgraph := range req.CompositeContents {
		subgraph := csubgraph.Subgraph
		v, err := encoding.DefaultEncoding.EncodeSubgraph(subgraph)
		if err != nil {
			return wrapError("encode csubgraph.subgraph", err)
		}
		if err := batch.Put([]byte(gid), v); err != nil {
			return wrapError("put csubgraph.subgraph", err)
		}
		writeRecord.AddSubgraph(subgraph, len(v)+len(gid))
		if m != nil {
			m.AddSubgraph(len(v) + len(gid))
		}
	}

	if WriteMetadata {
		metaBatch := g.metaDB.NewBatch()
		writeRecord.EndTime = time.Now()
		metadata.AppendWriteRecords(writeRecord)
		_, err := metadata.Write(metaBatch)
		if err != nil {
			return wrapError("write metadata", err)
		}
		if err := metaBatch.Write(); err != nil {
			return wrapError("metaDB batch write", err)
		}
	}

	if err := batch.Write(); err != nil {
		return wrapError("batch write", err)
	}
	return nil
}
