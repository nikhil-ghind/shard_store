package hashring

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const defaultVirtualNodes = 150

type ConsistentHashRing struct {
	mu           sync.RWMutex
	ring         []uint32
	nodeMap      map[uint32]string // hash -> physical node ID
	physicalNodes map[string]bool
	virtualNodes int
}

func NewConsistentHashRing() *ConsistentHashRing {
	return &ConsistentHashRing{
		nodeMap:       make(map[uint32]string),
		physicalNodes: make(map[string]bool),
		virtualNodes:  defaultVirtualNodes,
	}
}

func (r *ConsistentHashRing) hashKey(key string) uint32 {
	h := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}

func (r *ConsistentHashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.physicalNodes[nodeID] {
		return
	}
	r.physicalNodes[nodeID] = true

	for i := 0; i < r.virtualNodes; i++ {
		vkey := fmt.Sprintf("%s#%d", nodeID, i)
		hash := r.hashKey(vkey)
		r.ring = append(r.ring, hash)
		r.nodeMap[hash] = nodeID
	}
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
}

func (r *ConsistentHashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.physicalNodes[nodeID] {
		return
	}
	delete(r.physicalNodes, nodeID)

	newRing := make([]uint32, 0, len(r.ring))
	for _, hash := range r.ring {
		if r.nodeMap[hash] == nodeID {
			delete(r.nodeMap, hash)
		} else {
			newRing = append(newRing, hash)
		}
	}
	r.ring = newRing
}

// GetNodes returns up to replicationFactor distinct physical nodes for the given key.
func (r *ConsistentHashRing) GetNodes(key string, replicationFactor int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return nil
	}

	hash := r.hashKey(key)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= hash })
	if idx == len(r.ring) {
		idx = 0
	}

	seen := make(map[string]bool)
	var nodes []string

	for i := 0; i < len(r.ring) && len(nodes) < replicationFactor; i++ {
		pos := (idx + i) % len(r.ring)
		nodeID := r.nodeMap[r.ring[pos]]
		if !seen[nodeID] {
			seen[nodeID] = true
			nodes = append(nodes, nodeID)
		}
	}
	return nodes
}

func (r *ConsistentHashRing) GetAllNodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]string, 0, len(r.physicalNodes))
	for n := range r.physicalNodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (r *ConsistentHashRing) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.physicalNodes)
}
