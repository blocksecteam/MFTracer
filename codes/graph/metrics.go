package graph

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/olekukonko/tablewriter"
)

type counter uint64

func (c counter) String() string {
	return fmt.Sprintf("%d", c)
}

func (c counter) Uint64() uint64 {
	return uint64(c)
}

type StatSize struct {
	size  common.StorageSize
	count counter
}

func (s *StatSize) Add(size common.StorageSize, len counter) {
	s.size += size
	s.count += len
}

func (s *StatSize) Size() string {
	return s.size.String()
}

func (s *StatSize) RawSize() float64 {
	return float64(s.size)
}

func (s *StatSize) Count() string {
	return s.count.String()
}

func (s *StatSize) RawCount() uint64 {
	return s.count.Uint64()
}

func (s *StatSize) Reset() {
	s.size = 0
	s.count = 0
}

type StatTime struct {
	total common.PrettyDuration
	count counter
}

func (s *StatTime) Add(t time.Duration, len counter) {
	s.total += common.PrettyDuration(t)
	s.count += len
}

func (s *StatTime) Mean(t time.Duration) string {
	return (s.total / common.PrettyDuration(s.count)).String()
}

func (s *StatTime) Total() string {
	return s.total.String()
}

func (s *StatTime) Count() string {
	return s.count.String()
}

func (s *StatTime) RawTotal() time.Duration {
	return time.Duration(s.total)
}

func (s *StatTime) RawCount() uint64 {
	return s.count.Uint64()
}

func (s *StatTime) Reset() {
	s.total = 0
	s.count = 0
}

type WriteMetrics struct {
	subgraphs StatSize
	txs       StatSize
	transfers StatSize

	txExtra  StatSize
	nodeMeta StatSize
	meta     StatSize
}

func (m *WriteMetrics) AddSubgraph(s int) {
	m.subgraphs.Add(common.StorageSize(s), 1)
}

func (m *WriteMetrics) AddTx(s int, len int) {
	m.txs.Add(common.StorageSize(s), counter(len))
}

func (m *WriteMetrics) AddTransfer(s int, len int) {
	m.transfers.Add(common.StorageSize(s), counter(len))
}

func (m *WriteMetrics) AddMeta(size int) {
	m.meta.Add(common.StorageSize(size), 1)
}

func (m *WriteMetrics) AddNodeMeta(size int) {
	m.nodeMeta.Add(common.StorageSize(size), 1)
}

func (m *WriteMetrics) AddTxMeta(size int) {
	m.txExtra.Add(common.StorageSize(size), 1)
}

func (m *WriteMetrics) Show() {
	stats := [][]string{
		{"Subgraph", m.subgraphs.Size(), m.subgraphs.Count()},
		{"Txs", m.txs.Size(), m.txs.Count()},
		{"Transfers", m.transfers.Size(), m.transfers.Count()},
		// {"TxExtra", m.txExtra.Size(), m.txExtra.Count()},
		// {"NodeMeta", m.nodeMeta.Size(), m.nodeMeta.Count()},
		{"Metadata", m.meta.Size(), m.meta.Count()},
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Category", "Size", "Items"})
	table.AppendBulk(stats)
	table.Render()
}

func (m *WriteMetrics) Log() {
	ftoa := func(v float64) string { return strconv.FormatFloat(v, 'f', 5, 64) }
	log.Info("write metrics",
		"subgraphCount", m.subgraphs.RawCount(), "subgraphSize", ftoa(m.subgraphs.RawSize()),
		"txCount", m.txs.RawCount(), "txSize", ftoa(m.txs.RawSize()),
		"transferCount", m.transfers.RawCount(), "transferSize", ftoa(m.transfers.RawSize()),
	)
}

type QueryMetrics struct {
	sync.Mutex

	// Time of stages
	Index     StatTime
	Subgraphs StatTime
	Transfers StatTime
	Txs       StatTime

	// Size of Stages
	IndexBlocks  int
	IndexCount   int
	IndexSize    StatSize
	SubgraphSize StatSize
	TransferSize StatSize
	TxSize       StatSize

	// DB metrics
	DBGet       StatTime
	DBGetSize   StatSize
	DBRangeScan StatSize
}

func (m *QueryMetrics) Reset() {
	m.Index.Reset()
	m.Subgraphs.Reset()
	m.Transfers.Reset()
	m.Txs.Reset()

	m.IndexSize.Reset()
	m.SubgraphSize.Reset()
	m.TransferSize.Reset()
	m.TxSize.Reset()

	m.DBGet.Reset()
}

func (m *QueryMetrics) AddIndex(duration time.Duration, size, count, blocks int) {
	m.Lock()
	defer m.Unlock()

	m.Index.Add(duration, 1)
	m.IndexSize.Add(common.StorageSize(size), 1)
	m.IndexBlocks += blocks
	m.IndexCount += count
}

func (m *QueryMetrics) AddSubgraph(duration time.Duration, size int, len int) {
	m.Lock()
	defer m.Unlock()

	m.Subgraphs.Add(duration, counter(len))
	m.SubgraphSize.Add(common.StorageSize(size), counter(len))
}

func (m *QueryMetrics) AddTransfer(duration time.Duration, size int, len int) {
	m.Lock()
	defer m.Unlock()

	m.Transfers.Add(duration, counter(len))
	m.TransferSize.Add(common.StorageSize(size), counter(len))
}

func (m *QueryMetrics) AddTx(duration time.Duration, size int, len int) {
	m.Lock()
	defer m.Unlock()

	m.Txs.Add(duration, counter(len))
	m.TxSize.Add(common.StorageSize(size), counter(len))
}

func (m *QueryMetrics) AddDBGet(d time.Duration, size int) {
	m.Lock()
	defer m.Unlock()

	m.DBGet.Add(d, 1)
	m.DBGetSize.Add(common.StorageSize(size), 1)
}

func (m *QueryMetrics) AddDBRangeScan(size int) {
	m.Lock()
	defer m.Unlock()

	m.DBRangeScan.Add(common.StorageSize(size), 1)
}

func (m *QueryMetrics) Show() {
	stats := [][]string{
		{"Index", m.Index.Total(), m.Index.Count()},
		{"Subgraphs", m.Subgraphs.Total(), m.Subgraphs.Count()},
		{"Transfers", m.Transfers.Total(), m.Transfers.Count()},
		{"Txs", m.Txs.Total(), m.Txs.Count()},
		{"DBGet", m.DBGet.Total(), m.DBGet.Count()},
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Category", "Total", "Count"})
	table.AppendBulk(stats)
	table.Render()
}
