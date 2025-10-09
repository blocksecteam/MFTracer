package flow

import (
	"sort"
	"transfer-graph/model"
	"transfer-graph/search"

	"github.com/ethereum/go-ethereum/common"
)

type FlowDigest struct {
	From        string
	To          string
	Value       float64
	EdgePointer uint64
}

type FlowEdges interface {
	flow(activity flowActivity) []*FlowDigest
	Finished() bool
	AtPointer(pointer uint64) (*model.Tx, *model.Transfer)
}

type flow interface {
	fValue() float64
}

type FlowNode interface {
	new(address string) FlowNode
	in(value flow)
	out(value float64) flow
	source(value float64) flow
	TotalI() float64
	TotalO() float64
	Address() string
}

type flowActivity map[string]struct{}

func (fa flowActivity) check(addr common.Address) bool {
	_, ok := fa[string(addr.Bytes())]
	return ok
}

func (fa flowActivity) add(addr common.Address) {
	fa[string(addr.Bytes())] = struct{}{}
}

type FlowGraph struct {
	Nodes map[string]FlowNode //address -> node struct
	Edges FlowEdges

	motherNode   FlowNode
	srcs         map[string]struct{}
	dess         map[string]struct{}
	activity     flowActivity
	leachDigests []*FlowDigest
}

func NewFlowGraph(motherNode FlowNode, edges FlowEdges, srcs, dess []string) *FlowGraph {
	ret := &FlowGraph{
		Nodes:        make(map[string]FlowNode),
		Edges:        edges,
		motherNode:   motherNode,
		srcs:         make(map[string]struct{}, len(srcs)),
		dess:         make(map[string]struct{}, len(dess)),
		activity:     make(flowActivity),
		leachDigests: make([]*FlowDigest, 0),
	}
	for _, src := range srcs {
		ret.activate(src)
		ret.srcs[src] = struct{}{}
	}
	for _, des := range dess {
		ret.dess[des] = struct{}{}
	}
	return ret
}

func (fg *FlowGraph) activate(address string) {
	fg.Nodes[address] = fg.motherNode.new(address)
	fg.activity[address] = struct{}{}
}

func (fg *FlowGraph) FlowByStep(step int) {
	for i := 0; !fg.Edges.Finished() && i < step; i++ {
		ds := fg.Edges.flow(fg.activity)
		for _, d := range ds {
			if _, ok := fg.activity[d.From]; !ok {
				continue
			}
			if _, ok := fg.Nodes[d.To]; !ok {
				fg.activate(d.To)
			}
			if _, isSrc := fg.srcs[d.From]; isSrc {
				fvalue := fg.Nodes[d.From].source(d.Value)
				fg.Nodes[d.To].in(fvalue)
			} else {
				fvalue := fg.Nodes[d.From].out(d.Value)
				fg.Nodes[d.To].in(fvalue)
			}
			fg.leachDigests = append(fg.leachDigests, d)
		}
	}
}

func (fg *FlowGraph) FlowToEnd() {
	for !fg.Edges.Finished() {
		ds := fg.Edges.flow(fg.activity)
		for _, d := range ds {
			/*
				if strings.Compare(d.To, utils.AddrToAddrString(utils.WETHAddress)) == 0 {
					continue
				}
				if strings.Compare(d.To, utils.AddrToAddrString(common.HexToAddress("0x0000000000000000000000000000000000000002"))) == 0 ||
					strings.Compare(d.To, utils.AddrToAddrString(common.HexToAddress("0x0000000000000000000000000000000000000001"))) == 0 {
					continue
				}
			*/
			if _, ok := fg.activity[d.From]; !ok {
				continue
			}
			if _, ok := fg.Nodes[d.To]; !ok {
				fg.activate(d.To)
			}
			if _, isSrc := fg.srcs[d.From]; isSrc {
				fvalue := fg.Nodes[d.From].source(d.Value)
				fg.Nodes[d.To].in(fvalue)
			} else {
				fvalue := fg.Nodes[d.From].out(d.Value)
				fg.Nodes[d.To].in(fvalue)
			}
			fg.leachDigests = append(fg.leachDigests, d)
		}
	}
}

func (fg *FlowGraph) FlowTillDes() {
	for !fg.Edges.Finished() {
		ds := fg.Edges.flow(fg.activity)
		for _, d := range ds {
			if _, ok := fg.activity[d.From]; !ok {
				continue
			}
			if _, ok := fg.Nodes[d.To]; !ok {
				fg.activate(d.To)
			}
			if _, isSrc := fg.srcs[d.From]; isSrc {
				fvalue := fg.Nodes[d.From].source(d.Value)
				fg.Nodes[d.To].in(fvalue)
			} else {
				fvalue := fg.Nodes[d.From].out(d.Value)
				fg.Nodes[d.To].in(fvalue)
			}
			fg.leachDigests = append(fg.leachDigests, d)
			if _, isDes := fg.dess[d.To]; isDes {
				break
			}
		}
	}
}

func (fg *FlowGraph) TopNodes(top int) map[string]FlowNode {
	allForI := make([]FlowNode, 0, len(fg.Nodes))
	allForO := make([]FlowNode, 0, len(fg.Nodes))
	for _, node := range fg.Nodes {
		allForI = append(allForI, node)
		allForO = append(allForO, node)
	}
	sort.Slice(allForI, func(i, j int) bool {
		return allForI[i].TotalI() > allForI[j].TotalI()
	})
	sort.Slice(allForO, func(i, j int) bool {
		return allForO[i].TotalO() > allForO[j].TotalO()
	})
	ret := make(map[string]FlowNode)
	for i := 0; i < top && i < len(fg.Nodes); i++ {
		ret[allForI[i].Address()] = allForI[i]
		ret[allForO[i].Address()] = allForO[i]
	}
	return ret
}

type ValueEdge struct {
	Tx  *model.Tx
	Ts  *model.Transfer
	Val float64
}

func (ve *ValueEdge) FlowDigest() FlowDigest {
	if ve.Tx != nil {
		return FlowDigest{
			From:  string(ve.Tx.From.Bytes()),
			To:    string(ve.Tx.To.Bytes()),
			Value: ve.Val,
		}
	} else if ve.Ts != nil {
		return FlowDigest{
			From:  string(ve.Ts.From.Bytes()),
			To:    string(ve.Ts.To.Bytes()),
			Value: ve.Val,
		}
	} else {
		return FlowDigest{}
	}
}

func (a *ValueEdge) After(bi search.MEdge) bool {
	b := bi.(*ValueEdge)
	var aPos, bPos uint64
	if a.Tx != nil {
		aPos = a.Tx.Pos()
	} else if a.Ts != nil {
		aPos = a.Ts.Pos
	} else {
		aPos = 0
	}
	if b.Tx != nil {
		bPos = b.Tx.Pos()
	} else if b.Ts != nil {
		bPos = b.Ts.Pos
	} else {
		bPos = 0
	}
	return aPos >= bPos
}

func (a *ValueEdge) Before(bi search.MEdge) bool {
	b := bi.(*ValueEdge)
	var aPos, bPos uint64
	if a.Tx != nil {
		aPos = a.Tx.Pos()
	} else if a.Ts != nil {
		aPos = a.Ts.Pos
	} else {
		aPos = 0
	}
	if b.Tx != nil {
		bPos = b.Tx.Pos()
	} else if b.Ts != nil {
		bPos = b.Ts.Pos
	} else {
		bPos = 0
	}
	return aPos <= bPos
}

func (a *ValueEdge) TimeDelta(bi search.MEdge) int64 {
	b := bi.(*ValueEdge)
	var aPos, bPos uint64
	if a.Tx != nil {
		aPos = a.Tx.Pos()
	} else if a.Ts != nil {
		aPos = a.Ts.Pos
	} else {
		aPos = 0
	}
	if b.Tx != nil {
		bPos = b.Tx.Pos()
	} else if b.Ts != nil {
		bPos = b.Ts.Pos
	} else {
		bPos = 0
	}
	if aPos > bPos {
		return int64(aPos - bPos)
	} else {
		return -int64(bPos - aPos)
	}
}

func (ve *ValueEdge) From() string {
	if ve.Tx != nil {
		return string(ve.Tx.From.Bytes())
	} else if ve.Ts != nil {
		return string(ve.Ts.From.Bytes())
	} else {
		return ""
	}
}

func (ve *ValueEdge) To() string {
	if ve.Tx != nil {
		return string(ve.Tx.To.Bytes())
	} else if ve.Ts != nil {
		return string(ve.Ts.To.Bytes())
	} else {
		return ""
	}
}

func (ve *ValueEdge) Value() float64 {
	return ve.Val
}

func (ve *ValueEdge) Pos() uint64 {
	if ve.Tx != nil {
		return ve.Tx.Pos()
	} else if ve.Ts != nil {
		return ve.Ts.Pos
	} else {
		return 0
	}
}

func (ve *ValueEdge) Type() uint16 {
	if ve.Tx != nil {
		return 0
	} else if ve.Ts != nil {
		return ve.Ts.Type
	} else {
		return 0
	}
}

func (fg *FlowGraph) Leach(addrFilterS []string, addrFilterM map[string]struct{}, addrFilterN map[string]FlowNode, additional []string) ([]search.MEdge, map[string]FlowNode) {
	retEdges := make([]search.MEdge, 0)
	retNodes := make(map[string]FlowNode)
	var addrFilter map[string]struct{}
	if addrFilterS != nil {
		addrFilter = make(map[string]struct{}, len(addrFilterS))
		for _, addr := range addrFilterS {
			addrFilter[addr] = struct{}{}
		}
	} else if addrFilterM != nil {
		addrFilter = addrFilterM
	} else if addrFilterN != nil {
		addrFilter = make(map[string]struct{}, len(addrFilterN))
		for addr := range addrFilterN {
			addrFilter[addr] = struct{}{}
		}
	} else {
		addrFilter = fg.activity
	}
	/*
		// fg.srcs and fg.dess should be added to addrFilter
		for addr := range fg.srcs {
			addrFilter[addr] = struct{}{}
		}
		for addr := range fg.dess {
			addrFilter[addr] = struct{}{}
		}
	*/
	for _, addr := range additional {
		addrFilter[addr] = struct{}{}
	}
	for _, d := range fg.leachDigests {
		tx, ts := fg.Edges.AtPointer(d.EdgePointer)
		if tx != nil {
			_, okf := addrFilter[string(tx.From.Bytes())]
			_, okt := addrFilter[string(tx.To.Bytes())]
			if okf && okt {
				/*
					if fNode, ok := fg.Nodes[string(tx.From.Bytes())]; !ok || (fNode.TotalI() == 0 && fNode.TotalO() == 0) {
						if tNode, ok := fg.Nodes[string(tx.To.Bytes())]; !ok || (tNode.TotalI() == 0 && tNode.TotalO() == 0) {
							continue
						}
					}
				*/
				retEdges = append(retEdges, &ValueEdge{
					Tx:  tx,
					Val: d.Value,
				})
				retNodes[string(tx.From.Bytes())] = fg.Nodes[string(tx.From.Bytes())]
				retNodes[string(tx.To.Bytes())] = fg.Nodes[string(tx.To.Bytes())]
			}
		} else if ts != nil {
			_, okf := addrFilter[string(ts.From.Bytes())]
			_, okt := addrFilter[string(ts.To.Bytes())]
			if okf && okt {
				/*
					if fNode, ok := fg.Nodes[string(ts.From.Bytes())]; !ok || (fNode.TotalI() == 0 && fNode.TotalO() == 0) {
						if tNode, ok := fg.Nodes[string(ts.To.Bytes())]; !ok || (tNode.TotalI() == 0 && tNode.TotalO() == 0) {
							continue
						}
					}
				*/
				retEdges = append(retEdges, &ValueEdge{
					Ts:  ts,
					Val: d.Value,
				})
				retNodes[string(ts.From.Bytes())] = fg.Nodes[string(ts.From.Bytes())]
				retNodes[string(ts.To.Bytes())] = fg.Nodes[string(ts.To.Bytes())]
			}
		}
	}
	return retEdges, retNodes
}

func (fg *FlowGraph) Free() {
	fg.Nodes = nil
	fg.activity = nil
	fg.leachDigests = nil
	fg.srcs = nil
	fg.dess = nil
	fg.Edges = nil
}
