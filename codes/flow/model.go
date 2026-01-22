package flow

import (
	"math"
	"sort"
	"transfer-graph-evm/model"
	"transfer-graph-evm/search"
)

type FlowDigest struct {
	From        string
	To          string
	Value       float64
	EdgePointer uint64
	UsedValue   float64
	Age         int
}

type FlowEdges interface {
	flow(activity flowActivity) []*FlowDigest
	Finished() bool
	AtPointer(pointer uint64) (*model.Tx, *model.Transfer)
}

type flow interface {
	fValue() float64
	fAge() int
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

func (fa flowActivity) check(addr model.Address) bool {
	_, ok := fa[string(addr.Bytes())]
	return ok
}

func (fa flowActivity) add(addr model.Address) {
	fa[string(addr.Bytes())] = struct{}{}
}

type FlowGraph struct {
	Nodes map[string]FlowNode //address -> node struct
	Edges FlowEdges

	motherNode   FlowNode
	srcs         map[string]struct{}
	dess         map[string]struct{}
	activity     flowActivity
	LeachDigests []*FlowDigest
}

func NewFlowGraph(motherNode FlowNode, edges FlowEdges, srcs, dess []string) *FlowGraph {
	ret := &FlowGraph{
		Nodes:        make(map[string]FlowNode),
		Edges:        edges,
		motherNode:   motherNode,
		srcs:         make(map[string]struct{}, len(srcs)),
		dess:         make(map[string]struct{}, len(dess)),
		activity:     make(flowActivity),
		LeachDigests: make([]*FlowDigest, 0),
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

func (fg *FlowGraph) ResetEdges(newEdges FlowEdges) {
	fg.Edges = newEdges
	fg.LeachDigests = nil
}

func (fg *FlowGraph) WhatIsMotherNode() FlowNode {
	switch t := fg.motherNode.(type) {
	case *ThresholdFlowNode:
		return &ThresholdFlowNode{
			Config: t.Config,
		}
	case *ThresholdAgeFlowNode:
		return &ThresholdAgeFlowNode{
			Config: t.Config,
		}
	case *ThresholdAgeLabelFlowNode:
		return &ThresholdAgeLabelFlowNode{
			Config: &ThresholdAgeLabelFlowNodeConfig{
				Threshold:  t.Config.Threshold,
				AgeLimit:   t.Config.AgeLimit,
				LabelLimit: t.Config.LabelLimit,
			},
		}
	default:
		return nil
	}
}

func (fg *FlowGraph) SrcAddresses() []string {
	srcAddrs := make([]string, 0, len(fg.srcs))
	for addr := range fg.srcs {
		srcAddrs = append(srcAddrs, addr)
	}
	return srcAddrs
}

func (fg *FlowGraph) DesAddresses() []string {
	desAddrs := make([]string, 0, len(fg.dess))
	for addr := range fg.dess {
		desAddrs = append(desAddrs, addr)
	}
	return desAddrs
}

func (fg *FlowGraph) FlowByStep(step int) {
	for i := 0; !fg.Edges.Finished() && i < step; i++ {
		ds := fg.Edges.flow(fg.activity)
		for _, d := range ds {
			if _, ok := fg.activity[d.From]; !ok {
				continue
			}
			var fvalue flow
			if _, isSrc := fg.srcs[d.From]; isSrc {
				fvalue = fg.Nodes[d.From].source(d.Value)
			} else {
				fvalue = fg.Nodes[d.From].out(d.Value)
			}
			if fvalue.fValue() == 0 {
				continue
			}
			if _, ok := fg.Nodes[d.To]; !ok {
				fg.activate(d.To)
			}
			fg.Nodes[d.To].in(fvalue)
			d.UsedValue = fvalue.fValue()
			d.Age = fvalue.fAge()
			fg.LeachDigests = append(fg.LeachDigests, d)
		}
	}
}

func (fg *FlowGraph) FlowToEnd() {
	for !fg.Edges.Finished() {
		ds := fg.Edges.flow(fg.activity)
		for _, d := range ds {
			if _, ok := fg.activity[d.From]; !ok {
				continue
			}
			var fvalue flow
			if _, isSrc := fg.srcs[d.From]; isSrc {
				fvalue = fg.Nodes[d.From].source(d.Value)
			} else {
				fvalue = fg.Nodes[d.From].out(d.Value)
			}
			if fvalue.fValue() == 0 {
				continue
			}
			if _, ok := fg.Nodes[d.To]; !ok {
				fg.activate(d.To)
			}
			fg.Nodes[d.To].in(fvalue)
			d.UsedValue = fvalue.fValue()
			d.Age = fvalue.fAge()
			fg.LeachDigests = append(fg.LeachDigests, d)
		}
	}
}

func (fg *FlowGraph) TotalVolume() float64 {
	var total float64
	for _, node := range fg.Nodes {
		if _, isSrc := fg.srcs[node.Address()]; isSrc {
			total += node.TotalO()
		}
	}
	return total
}

func (fg *FlowGraph) TopNodes(top int, compare func(a, b FlowNode) bool) []FlowNode {
	nodes := make([]FlowNode, 0, len(fg.Nodes))
	for _, node := range fg.Nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return compare(nodes[i], nodes[j])
	})
	return nodes[:int(math.Min(float64(top), float64(len(nodes))))] // Fix the slice bounds
}

type ValueEdge struct {
	Tx      *model.Tx
	Ts      *model.Transfer
	Val     float64
	UsedVal float64
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

func (fg *FlowGraph) Leach() []search.MEdge {
	retEdges := make([]search.MEdge, 0)
	for _, d := range fg.LeachDigests {
		tx, ts := fg.Edges.AtPointer(d.EdgePointer)
		if tx != nil {
			retEdges = append(retEdges, &ValueEdge{
				Tx:      tx,
				Val:     d.Value,
				UsedVal: d.UsedValue,
			})
		} else if ts != nil {
			retEdges = append(retEdges, &ValueEdge{
				Ts:      ts,
				Val:     d.Value,
				UsedVal: d.UsedValue,
			})
		}
	}
	return retEdges
}

func (fg *FlowGraph) LeachOneTransferNative(i int) (*model.Transfer, float64, int) {
	d := fg.LeachDigests[i]
	_, ts := fg.Edges.AtPointer(d.EdgePointer)
	return ts, d.UsedValue, d.Age
}

func (fg *FlowGraph) Free() {
	fg.Nodes = nil
	fg.activity = nil
	fg.LeachDigests = nil
	fg.srcs = nil
	fg.dess = nil
	fg.Edges = nil
}
