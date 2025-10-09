package flow

import (
	"encoding/binary"
	"math"
	"sort"
)

const (
//RPFlowDecayFactor = 0.1 //note: in range [0, 1)

// LabelSRPFlowAgeLimit         int = 10
// LabelSRPFlowLabelLengthLimit int = 2000
// LabelSRPFlowActiveThreshold  float64 = 0.00000001
// LabelSRPFlowActiveThreshold float64 = 1
)

type RPFlow float64

func (f RPFlow) fValue() float64 {
	return float64(f)
}

type RPFlowConfig float64 //RPFlowDecayFactor

type RPFlowNode struct {
	bucket  float64
	totalI  float64
	totalO  float64
	address string

	Config RPFlowConfig
}

func (mother *RPFlowNode) new(address string) FlowNode {
	return &RPFlowNode{
		address: address,
		Config:  mother.Config,
	}
}

func (n *RPFlowNode) in(value flow) {
	//pvalue := (value.fValue() / (1 - RPFlowDecayFactor))
	pvalue := value.fValue()
	n.bucket += pvalue
	n.totalI += value.fValue()
}

func (n *RPFlowNode) out(value float64) flow {
	max := n.bucket * (1 - float64(n.Config))
	var rvalue float64
	if value < max {
		rvalue = value
	} else {
		rvalue = max
	}
	n.bucket -= rvalue
	n.totalO += rvalue
	return RPFlow(rvalue)
}

func (n *RPFlowNode) tryOut(value float64) flow {
	max := n.bucket * (1 - float64(n.Config))
	if value < max {
		return RPFlow(value)
	} else {
		return RPFlow(max)
	}
}

func (n *RPFlowNode) source(value float64) flow {
	n.totalO += value
	return RPFlow(value)
}

func (n *RPFlowNode) TotalI() float64 {
	return n.totalI
}

func (n *RPFlowNode) TotalO() float64 {
	return n.totalO
}

func (n *RPFlowNode) Address() string {
	return n.address
}

type addressLabel string

func (al addressLabel) address() string {
	return string([]byte(al)[8:])
}

func (al addressLabel) value() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64([]byte(al)[:8]))
}

func makeAddressLabel(address string, value float64) addressLabel {
	ret := make([]byte, 8+len(address))
	binary.LittleEndian.PutUint64(ret[:8], math.Float64bits(value))
	copy(ret[8:], []byte(address))
	return addressLabel(ret)
}

type LabelSRPFlow struct {
	value  float64
	labels []string
	age    int
}

func (l *LabelSRPFlow) fValue() float64 {
	return l.value
}

type LabelSRPFlowConfig struct {
	RpflowConfig     RPFlowConfig
	AgeLimit         int
	LabelLengthLimit int
	ActiveThreshold  float64
}

type LabelSRPFlowNode struct {
	rpnode  RPFlowNode
	labels  []addressLabel
	flowAge int

	Config *LabelSRPFlowConfig
}

func (mother *LabelSRPFlowNode) new(address string) FlowNode {
	return &LabelSRPFlowNode{
		rpnode: RPFlowNode{
			address: address,
			Config:  mother.Config.RpflowConfig,
		},
		labels:  nil,
		flowAge: 0,

		Config: mother.Config,
	}
}

func (n *LabelSRPFlowNode) in(value flow) {
	flow := value.(*LabelSRPFlow)
	if flow.age > n.Config.AgeLimit || flow.value < n.Config.ActiveThreshold {
		return
	}
	if n.flowAge >= n.Config.AgeLimit {
		n.labels = make([]addressLabel, 0, n.Config.LabelLengthLimit)
		n.flowAge = 0
		n.rpnode.bucket = 0
	}
	if n.labels == nil {
		n.labels = make([]addressLabel, 0, n.Config.LabelLengthLimit)
	}
	for _, l := range flow.labels {
		n.labels = append(n.labels, makeAddressLabel(l, flow.value))
	}
	if len(n.labels) > n.Config.LabelLengthLimit {
		vmap := make(map[string]float64)
		for _, l := range n.labels {
			vmap[l.address()] = vmap[l.address()] + l.value()
		}
		n.labels = make([]addressLabel, 0, n.Config.LabelLengthLimit)
		for a, v := range vmap {
			n.labels = append(n.labels, makeAddressLabel(a, v))
		}
		if len(n.labels) > n.Config.LabelLengthLimit {
			sort.Slice(n.labels, func(i, j int) bool {
				return n.labels[i].value() < n.labels[j].value()
			})
			n.labels = n.labels[len(n.labels)-n.Config.LabelLengthLimit:]
		}
	}
	if flow.age < n.flowAge {
		n.flowAge = flow.age
	}
	n.rpnode.in(value)
}

func (n *LabelSRPFlowNode) out(value float64) flow {
	if ovalue := n.rpnode.tryOut(value); ovalue.fValue() < n.Config.ActiveThreshold {
		return &LabelSRPFlow{value: 0}
	}
	ret := &LabelSRPFlow{
		value:  n.rpnode.out(value).fValue(),
		labels: make([]string, len(n.labels)+1),
		age:    n.flowAge + 1,
	}
	for i, l := range n.labels {
		ret.labels[i] = l.address()
	}
	ret.labels[len(n.labels)] = n.rpnode.Address()
	return ret
}

func (n *LabelSRPFlowNode) source(value float64) flow {
	n.rpnode.source(value)
	return &LabelSRPFlow{
		value:  value,
		labels: []string{n.rpnode.Address()},
		age:    0,
	}
}

func (n *LabelSRPFlowNode) TotalI() float64 {
	return n.rpnode.totalI
}

func (n *LabelSRPFlowNode) TotalO() float64 {
	return n.rpnode.totalO
}

func (n *LabelSRPFlowNode) Address() string {
	return n.rpnode.address
}

func (n *LabelSRPFlowNode) Labels() map[string]struct{} {
	ret := make(map[string]struct{}, len(n.labels))
	for _, l := range n.labels {
		ret[l.address()] = struct{}{}
	}
	return ret
}
