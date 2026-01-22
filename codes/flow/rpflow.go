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
	ONLY_EDGE = false
)

type RPFlow float64

func (f RPFlow) fValue() float64 {
	return float64(f)
}

func (f RPFlow) fAge() int {
	return 0
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

	pvalue float64
}

func (l *LabelSRPFlow) fValue() float64 {
	return l.value
}

func (l *LabelSRPFlow) fAge() int {
	return l.age
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
	if ONLY_EDGE {
		ret.labels = []string{n.rpnode.Address()}
		ret.pvalue = value
	}
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

func (n *LabelSRPFlowNode) LabelsWithValue() map[string]float64 {
	ret := make(map[string]float64, len(n.labels))
	for _, l := range n.labels {
		if _, ok := ret[l.address()]; !ok {
			ret[l.address()] = 0
		}
		ret[l.address()] += l.value()
	}
	return ret
}

type ThresholdFlow float64

func (f ThresholdFlow) fValue() float64 {
	return float64(f)
}

func (f ThresholdFlow) fAge() int {
	return 0
}

type ThresholdFlowConfig float64 //active threshold

type ThresholdFlowNode struct {
	bucket  float64
	totalI  float64
	totalO  float64
	address string

	Config ThresholdFlowConfig
}

func (mother *ThresholdFlowNode) new(address string) FlowNode {
	return &ThresholdFlowNode{
		address: address,
		Config:  mother.Config,
	}
}

func (n *ThresholdFlowNode) in(value flow) {
	pvalue := value.fValue()
	n.bucket += pvalue
	n.totalI += pvalue
}

func (n *ThresholdFlowNode) out(value float64) flow {
	rvalue := math.Min(n.bucket, value)
	if rvalue < float64(n.Config) {
		return ThresholdFlow(0)
	}
	n.bucket -= rvalue
	n.totalO += rvalue
	return ThresholdFlow(rvalue)
}

func (n *ThresholdFlowNode) source(value float64) flow {
	n.totalO += value
	return ThresholdFlow(value)
}

func (n *ThresholdFlowNode) TotalI() float64 {
	return n.totalI
}

func (n *ThresholdFlowNode) TotalO() float64 {
	return n.totalO
}

func (n *ThresholdFlowNode) Address() string {
	return n.address
}

type ThresholdAgeFlow [2]float64 //0: value, 1: age

func (f ThresholdAgeFlow) fValue() float64 {
	return f[0]
}

func (f ThresholdAgeFlow) fAge() int {
	return int(f[1])
}

type ThresholdAgeFlowNode struct {
	bucket  float64
	totalI  float64
	totalO  float64
	address string
	age     int

	Config *ThresholdAgeFlowNodeConfig
}

type ThresholdAgeFlowNodeConfig struct {
	Threshold float64
	AgeLimit  int
}

func (mother *ThresholdAgeFlowNode) new(address string) FlowNode {
	return &ThresholdAgeFlowNode{
		address: address,
		Config:  mother.Config,
	}
}

func (n *ThresholdAgeFlowNode) in(value flow) {
	v := value.(ThresholdAgeFlow)
	if n.bucket < n.Config.Threshold {
		n.age = v.fAge()
	} else {
		if a := v.fAge(); a < n.age {
			n.age = a
		}
	}
	n.bucket += v.fValue()
	n.totalI += v.fValue()
}

func (n *ThresholdAgeFlowNode) out(value float64) flow {
	rvalue := math.Min(n.bucket, value)
	if rvalue < n.Config.Threshold || n.age+1 > n.Config.AgeLimit {
		return ThresholdAgeLabelFlow(make([]byte, 16))
	}
	n.bucket -= rvalue
	n.totalO += rvalue
	return ThresholdAgeFlow([2]float64{rvalue, float64(n.age + 1)})
}

func (n *ThresholdAgeFlowNode) source(value float64) flow {
	ret := ThresholdAgeFlow([2]float64{0, 0})
	if value < n.bucket {
		ret = n.out(value).(ThresholdAgeFlow)
	}
	if ret.fValue() == 0 && ret.fAge() == 0 && value >= n.Config.Threshold {
		n.totalO += value
		ret = ThresholdAgeFlow([2]float64{value, 1})
	}
	return ret
}

func (n *ThresholdAgeFlowNode) TotalI() float64 {
	return n.totalI
}

func (n *ThresholdAgeFlowNode) TotalO() float64 {
	return n.totalO
}

func (n *ThresholdAgeFlowNode) Address() string {
	return n.address
}

type ThresholdAgeLabelFlowNode struct {
	bucket  float64
	totalI  float64
	totalO  float64
	address string
	age     int
	labels  []byte //address0|address1|...

	Config *ThresholdAgeLabelFlowNodeConfig
}

type ThresholdAgeLabelFlowNodeConfig struct {
	Threshold  float64
	AgeLimit   int
	LabelLimit int
}

type ThresholdAgeLabelFlow []byte //float64(value)|uint64(age)|address0|address1|...

func (f ThresholdAgeLabelFlow) fValue() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(f[:8]))
}

func (f ThresholdAgeLabelFlow) fAge() int {
	return int(binary.LittleEndian.Uint64(f[8:16]))
}

func (mother *ThresholdAgeLabelFlowNode) new(address string) FlowNode {
	return &ThresholdAgeLabelFlowNode{
		address: address,
		labels:  make([]byte, 0),
		Config:  mother.Config,
	}
}

func (n *ThresholdAgeLabelFlowNode) in(value flow) {
	v := value.(ThresholdAgeLabelFlow)
	if n.bucket < n.Config.Threshold {
		n.age = v.fAge()
		n.labels = n.labels[:0]
	} else {
		if a := v.fAge(); a < n.age {
			n.age = a
		}
	}
	n.labels = append(n.labels, v[16:]...)
	if len(n.labels)/len(n.address) > n.Config.LabelLimit {
		labelSet := make(map[string]struct{})
		for i := 0; i < len(n.labels); i += len(n.address) {
			labelSet[string(n.labels[i:i+len(n.address)])] = struct{}{}
		}
		n.labels = n.labels[:0]
		for l := range labelSet {
			n.labels = append(n.labels, []byte(l)...)
		}
	}
	if len(n.labels)/len(n.address) > n.Config.LabelLimit {
		oldLabelLimit := n.Config.LabelLimit
		n.Config = &ThresholdAgeLabelFlowNodeConfig{
			Threshold:  n.Config.Threshold,
			AgeLimit:   n.Config.AgeLimit,
			LabelLimit: oldLabelLimit * 2,
		}
	}
	n.bucket += v.fValue()
	n.totalI += v.fValue()
}

func (n *ThresholdAgeLabelFlowNode) out(value float64) flow {
	/*
		if n.age+1 > n.Config.AgeLimit {
			n.bucket = 0
		}
	*/
	rvalue := math.Min(n.bucket, value)
	if rvalue < n.Config.Threshold || n.age+1 > n.Config.AgeLimit {
		return ThresholdAgeLabelFlow(make([]byte, 16))
	}
	n.bucket -= rvalue
	n.totalO += rvalue
	ret := make([]byte, 16+len(n.labels)+len(n.address))
	binary.LittleEndian.PutUint64(ret[:8], math.Float64bits(rvalue))
	binary.LittleEndian.PutUint64(ret[8:16], uint64(n.age+1))
	copy(ret[16:], n.labels)
	copy(ret[16+len(n.labels):], []byte(n.address))
	return ThresholdAgeLabelFlow(ret)
}

func (n *ThresholdAgeLabelFlowNode) source(value float64) flow {
	ret := ThresholdAgeLabelFlow(make([]byte, 16))
	if value < n.bucket {
		ret = n.out(value).(ThresholdAgeLabelFlow)
	}
	if ret.fValue() == 0 && ret.fAge() == 0 && value >= n.Config.Threshold {
		n.totalO += value
		ret = make([]byte, 16+len(n.address))
		binary.LittleEndian.PutUint64(ret[:8], math.Float64bits(value))
		binary.LittleEndian.PutUint64(ret[8:16], uint64(1))
		copy(ret[16:], []byte(n.address))
	}
	return ret
}

func (n *ThresholdAgeLabelFlowNode) TotalI() float64 {
	return n.totalI
}

func (n *ThresholdAgeLabelFlowNode) TotalO() float64 {
	return n.totalO
}

func (n *ThresholdAgeLabelFlowNode) Address() string {
	return n.address
}

func (n *ThresholdAgeLabelFlowNode) CompactLabels() map[string]struct{} {
	ret := make(map[string]struct{})
	for i := 0; i < len(n.labels); i += len(n.address) {
		ret[string(n.labels[i:i+len(n.address)])] = struct{}{}
	}
	return ret
}

func (n *ThresholdAgeLabelFlowNode) Labels() []string {
	ret := make([]string, 0, len(n.labels)/len(n.address))
	for i := 0; i < len(n.labels); i += len(n.address) {
		ret = append(ret, string(n.labels[i:i+len(n.address)]))
	}
	return ret
}

func (n *ThresholdAgeLabelFlowNode) RawLabels() []byte {
	return n.labels
}
