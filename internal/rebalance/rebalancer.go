package rebalance

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/nikhilghind/shard-store/internal/hashring"
	"github.com/nikhilghind/shard-store/internal/metadata"
)

const (
	rebalanceInterval = 10 * time.Second
	replicationFactor = 3
)

type NodeHealth struct {
	Alive    bool
	Address  string
	LastSeen time.Time
}

type Rebalancer struct {
	ring      *hashring.ConsistentHashRing
	meta      *metadata.MetadataStore
	nodeHealth func() map[string]*NodeHealth
}

func NewRebalancer(
	ring *hashring.ConsistentHashRing,
	meta *metadata.MetadataStore,
	nodeHealth func() map[string]*NodeHealth,
) *Rebalancer {
	return &Rebalancer{
		ring:       ring,
		meta:       meta,
		nodeHealth: nodeHealth,
	}
}

func (r *Rebalancer) Start() {
	go func() {
		ticker := time.NewTicker(rebalanceInterval)
		defer ticker.Stop()
		for range ticker.C {
			r.rebalance()
		}
	}()
}

func (r *Rebalancer) rebalance() {
	health := r.nodeHealth()
	objects := r.meta.GetAllObjects()

	for _, obj := range objects {
		var liveReplicas []string
		var deadReplicas []string

		for _, nodeID := range obj.Replicas {
			nh, exists := health[nodeID]
			if exists && nh.Alive {
				liveReplicas = append(liveReplicas, nodeID)
			} else {
				deadReplicas = append(deadReplicas, nodeID)
			}
		}

		if len(deadReplicas) == 0 {
			continue
		}

		if len(liveReplicas) == 0 {
			log.Printf("CRITICAL: object %s/%s has no live replicas", obj.Bucket, obj.Key)
			continue
		}

		// Find new target nodes from the ring
		needed := replicationFactor - len(liveReplicas)
		if needed <= 0 {
			// Just update metadata to remove dead replicas
			r.meta.UpdateReplicas(obj.Bucket, obj.Key, liveReplicas)
			continue
		}

		candidates := r.ring.GetNodes(fmt.Sprintf("%s/%s", obj.Bucket, obj.Key), replicationFactor+len(deadReplicas))
		liveSet := make(map[string]bool)
		for _, n := range liveReplicas {
			liveSet[n] = true
		}

		var newTargets []string
		for _, c := range candidates {
			if liveSet[c] {
				continue
			}
			nh, exists := health[c]
			if exists && nh.Alive {
				newTargets = append(newTargets, c)
				if len(newTargets) >= needed {
					break
				}
			}
		}

		// Copy from a live replica to new targets
		sourceNode := liveReplicas[0]
		sourceAddr := health[sourceNode].Address

		for _, target := range newTargets {
			targetAddr := health[target].Address
			err := r.copyObject(sourceAddr, targetAddr, obj.Bucket, obj.Key)
			if err != nil {
				log.Printf("rebalance copy failed for %s/%s to %s: %v", obj.Bucket, obj.Key, target, err)
				continue
			}
			liveReplicas = append(liveReplicas, target)
			log.Printf("rebalanced %s/%s: copied to %s", obj.Bucket, obj.Key, target)
		}

		r.meta.UpdateReplicas(obj.Bucket, obj.Key, liveReplicas)
	}
}

func (r *Rebalancer) copyObject(srcAddr, dstAddr, bucket, key string) error {
	// GET from source
	resp, err := http.Get(fmt.Sprintf("http://%s/store/%s/%s", srcAddr, bucket, key))
	if err != nil {
		return fmt.Errorf("read from source: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("source returned %d", resp.StatusCode)
	}

	// PUT to destination
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s/store/%s/%s", dstAddr, bucket, key), resp.Body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("write to dest: %w", err)
	}
	defer putResp.Body.Close()
	io.Copy(io.Discard, putResp.Body)

	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusCreated {
		return fmt.Errorf("dest returned %d", putResp.StatusCode)
	}
	return nil
}
