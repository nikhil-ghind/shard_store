package coordinator

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nikhilghind/shard-store/internal/hashring"
	"github.com/nikhilghind/shard-store/internal/metadata"
	"github.com/nikhilghind/shard-store/internal/rebalance"
)

const (
	replicationFactor   = 3
	heartbeatTimeout    = 15 * time.Second
	healthCheckInterval = 5 * time.Second
)

type NodeInfo struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	Alive    bool      `json:"alive"`
	LastSeen time.Time `json:"last_seen"`
}

type Coordinator struct {
	mu         sync.RWMutex
	ring       *hashring.ConsistentHashRing
	meta       *metadata.MetadataStore
	nodes      map[string]*NodeInfo
	rebalancer *rebalance.Rebalancer

	// Prometheus metrics
	objectCount    prometheus.Gauge
	nodeCount      prometheus.Gauge
	putRequests    prometheus.Counter
	getRequests    prometheus.Counter
	deleteRequests prometheus.Counter
	replicaGauge   prometheus.Gauge
}

func NewCoordinator() *Coordinator {
	objectCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "shardstore_objects_total",
		Help: "Total number of objects stored",
	})
	nodeCount := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "shardstore_nodes_total",
		Help: "Total number of registered nodes",
	})
	putRequests := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "shardstore_put_requests_total",
		Help: "Total PUT requests",
	})
	getRequests := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "shardstore_get_requests_total",
		Help: "Total GET requests",
	})
	deleteRequests := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "shardstore_delete_requests_total",
		Help: "Total DELETE requests",
	})
	replicaGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "shardstore_under_replicated_objects",
		Help: "Number of under-replicated objects",
	})

	prometheus.MustRegister(objectCount, nodeCount, putRequests, getRequests, deleteRequests, replicaGauge)

	ring := hashring.NewConsistentHashRing()
	meta := metadata.NewMetadataStore()

	c := &Coordinator{
		ring:           ring,
		meta:           meta,
		nodes:          make(map[string]*NodeInfo),
		objectCount:    objectCount,
		nodeCount:      nodeCount,
		putRequests:    putRequests,
		getRequests:    getRequests,
		deleteRequests: deleteRequests,
		replicaGauge:   replicaGauge,
	}

	c.rebalancer = rebalance.NewRebalancer(ring, meta, c.getNodeHealth)
	return c
}

func (c *Coordinator) getNodeHealth() map[string]*rebalance.NodeHealth {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*rebalance.NodeHealth, len(c.nodes))
	for id, n := range c.nodes {
		result[id] = &rebalance.NodeHealth{
			Alive:    n.Alive,
			Address:  n.Address,
			LastSeen: n.LastSeen,
		}
	}
	return result
}

func (c *Coordinator) Start(port int) error {
	c.rebalancer.Start()
	go c.healthCheckLoop()

	mux := http.NewServeMux()

	// Bucket operations
	mux.HandleFunc("/buckets", c.handleBuckets)
	mux.HandleFunc("/buckets/", c.handleBucket)

	// Object operations
	mux.HandleFunc("/objects/", c.handleObject)

	// Cluster operations
	mux.HandleFunc("/cluster/status", c.handleClusterStatus)
	mux.HandleFunc("/cluster/nodes", c.handleClusterNodes)

	// Heartbeat
	mux.HandleFunc("/heartbeat", c.handleHeartbeat)

	// Prometheus metrics
	mux.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Coordinator listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func (c *Coordinator) healthCheckLoop() {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		aliveCount := 0
		for _, node := range c.nodes {
			if time.Since(node.LastSeen) > heartbeatTimeout {
				if node.Alive {
					log.Printf("Node %s marked as dead (last seen %v ago)", node.ID, time.Since(node.LastSeen))
					node.Alive = false
				}
			} else {
				aliveCount++
			}
		}
		c.nodeCount.Set(float64(aliveCount))
		c.mu.Unlock()
	}
}

func (c *Coordinator) handleBuckets(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		buckets := c.meta.ListBuckets()
		writeJSON(w, http.StatusOK, map[string]interface{}{"buckets": buckets})
		return
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (c *Coordinator) handleBucket(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/buckets/")
	if name == "" {
		http.Error(w, "bucket name required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		if err := c.meta.CreateBucket(name); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"bucket": name, "status": "created"})
	case http.MethodDelete:
		if err := c.meta.DeleteBucket(name); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"bucket": name, "status": "deleted"})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (c *Coordinator) handleObject(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/objects/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		http.Error(w, "path must be /objects/{bucket}/{key}", http.StatusBadRequest)
		return
	}
	bucket, key := parts[0], parts[1]

	switch r.Method {
	case http.MethodPut:
		c.putObject(w, r, bucket, key)
	case http.MethodGet:
		c.getObject(w, r, bucket, key)
	case http.MethodDelete:
		c.deleteObject(w, r, bucket, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (c *Coordinator) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	c.putRequests.Inc()

	if !c.meta.BucketExists(bucket) {
		http.Error(w, "bucket not found", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Compute checksum
	checksum := fmt.Sprintf("%x", md5.Sum(body))

	// Get target nodes from ring
	ringKey := fmt.Sprintf("%s/%s", bucket, key)
	targetNodes := c.ring.GetNodes(ringKey, replicationFactor)

	c.mu.RLock()
	var targets []*NodeInfo
	for _, nodeID := range targetNodes {
		if n, ok := c.nodes[nodeID]; ok && n.Alive {
			targets = append(targets, n)
		}
	}
	c.mu.RUnlock()

	if len(targets) == 0 {
		http.Error(w, "no healthy nodes available", http.StatusServiceUnavailable)
		return
	}

	// Fan-out PUT to all target nodes
	var wg sync.WaitGroup
	successNodes := make([]string, 0, len(targets))
	var successMu sync.Mutex

	for _, node := range targets {
		wg.Add(1)
		go func(n *NodeInfo) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s/store/%s/%s", n.Address, bucket, key)
			req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(string(body)))
			if err != nil {
				log.Printf("failed to create request to %s: %v", n.ID, err)
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("failed to PUT to node %s: %v", n.ID, err)
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
				successMu.Lock()
				successNodes = append(successNodes, n.ID)
				successMu.Unlock()
			}
		}(node)
	}
	wg.Wait()

	if len(successNodes) == 0 {
		http.Error(w, "failed to write to any node", http.StatusInternalServerError)
		return
	}

	meta := &metadata.ObjectMeta{
		Key:       key,
		Bucket:    bucket,
		Size:      int64(len(body)),
		Checksum:  checksum,
		Replicas:  successNodes,
		CreatedAt: time.Now(),
	}
	if err := c.meta.PutObject(meta); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	c.objectCount.Inc()
	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"bucket":   bucket,
		"key":      key,
		"size":     len(body),
		"checksum": checksum,
		"replicas": successNodes,
	})
}

func (c *Coordinator) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	c.getRequests.Inc()

	obj, err := c.meta.GetObject(bucket, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	c.mu.RLock()
	var sourceAddr string
	for _, nodeID := range obj.Replicas {
		if n, ok := c.nodes[nodeID]; ok && n.Alive {
			sourceAddr = n.Address
			break
		}
	}
	c.mu.RUnlock()

	if sourceAddr == "" {
		http.Error(w, "no healthy replica available", http.StatusServiceUnavailable)
		return
	}

	url := fmt.Sprintf("http://%s/store/%s/%s", sourceAddr, bucket, key)
	resp, err := http.Get(url)
	if err != nil {
		http.Error(w, "failed to read from storage node", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	w.Header().Set("X-Checksum", obj.Checksum)
	w.Header().Set("X-Replicas", strings.Join(obj.Replicas, ","))
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func (c *Coordinator) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	c.deleteRequests.Inc()

	obj, err := c.meta.GetObject(bucket, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	c.mu.RLock()
	var targets []*NodeInfo
	for _, nodeID := range obj.Replicas {
		if n, ok := c.nodes[nodeID]; ok {
			targets = append(targets, n)
		}
	}
	c.mu.RUnlock()

	// Fan-out DELETE
	var wg sync.WaitGroup
	for _, node := range targets {
		wg.Add(1)
		go func(n *NodeInfo) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s/store/%s/%s", n.Address, bucket, key)
			req, err := http.NewRequest(http.MethodDelete, url, nil)
			if err != nil {
				return
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("failed to DELETE from node %s: %v", n.ID, err)
				return
			}
			resp.Body.Close()
		}(node)
	}
	wg.Wait()

	c.meta.DeleteObject(bucket, key)
	c.objectCount.Dec()
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (c *Coordinator) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mu.RLock()
	nodes := make([]map[string]interface{}, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, map[string]interface{}{
			"id":        n.ID,
			"address":   n.Address,
			"alive":     n.Alive,
			"last_seen": n.LastSeen,
		})
	}
	c.mu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"nodes":       nodes,
		"total_nodes": len(nodes),
		"ring_nodes":  c.ring.NodeCount(),
		"buckets":     c.meta.ListBuckets(),
	})
}

func (c *Coordinator) handleClusterNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID      string `json:"id"`
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	c.nodes[req.ID] = &NodeInfo{
		ID:       req.ID,
		Address:  req.Address,
		Alive:    true,
		LastSeen: time.Now(),
	}
	c.mu.Unlock()

	c.ring.AddNode(req.ID)
	c.nodeCount.Inc()
	log.Printf("Node registered: %s at %s", req.ID, req.Address)

	writeJSON(w, http.StatusOK, map[string]string{"status": "registered", "id": req.ID})
}

func (c *Coordinator) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	c.mu.Lock()
	if node, ok := c.nodes[req.ID]; ok {
		node.LastSeen = time.Now()
		node.Alive = true
	}
	c.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
