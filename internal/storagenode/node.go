package storagenode

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const heartbeatInterval = 3 * time.Second

type StorageNode struct {
	ID             string
	DataDir        string
	CoordinatorURL string
	Port           int

	mu          sync.RWMutex
	objectCount int64
	diskUsage   int64

	// Prometheus metrics
	storedObjects prometheus.Gauge
	diskUsageGauge prometheus.Gauge
}

func NewStorageNode(id, dataDir, coordinatorURL string, port int) *StorageNode {
	storedObjects := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "storagenode_objects_total",
		Help:        "Total objects on this node",
		ConstLabels: prometheus.Labels{"node_id": id},
	})
	diskUsageGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "storagenode_disk_usage_bytes",
		Help:        "Disk usage in bytes",
		ConstLabels: prometheus.Labels{"node_id": id},
	})
	prometheus.MustRegister(storedObjects, diskUsageGauge)

	return &StorageNode{
		ID:             id,
		DataDir:        dataDir,
		CoordinatorURL: coordinatorURL,
		Port:           port,
		storedObjects:  storedObjects,
		diskUsageGauge: diskUsageGauge,
	}
}

func (n *StorageNode) Start() error {
	os.MkdirAll(n.DataDir, 0o755)

	if err := n.register(); err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}

	go n.heartbeatLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/store/", n.handleStore)
	mux.HandleFunc("/health", n.handleHealth)
	mux.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf(":%d", n.Port)
	log.Printf("Storage node %s listening on %s (data: %s)", n.ID, addr, n.DataDir)
	return http.ListenAndServe(addr, mux)
}

func (n *StorageNode) register() error {
	body, _ := json.Marshal(map[string]string{
		"id":      n.ID,
		"address": fmt.Sprintf("localhost:%d", n.Port),
	})
	resp, err := http.Post(n.CoordinatorURL+"/cluster/nodes", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registration returned %d", resp.StatusCode)
	}
	log.Printf("Registered with coordinator as %s", n.ID)
	return nil
}

func (n *StorageNode) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		body, _ := json.Marshal(map[string]string{"id": n.ID})
		resp, err := http.Post(n.CoordinatorURL+"/heartbeat", "application/json", bytes.NewReader(body))
		if err != nil {
			log.Printf("heartbeat failed: %v", err)
			continue
		}
		resp.Body.Close()
	}
}

func (n *StorageNode) handleStore(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/store/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		http.Error(w, "path must be /store/{bucket}/{key}", http.StatusBadRequest)
		return
	}
	bucket, key := parts[0], parts[1]

	switch r.Method {
	case http.MethodPut:
		n.putObject(w, r, bucket, key)
	case http.MethodGet:
		n.getObject(w, r, bucket, key)
	case http.MethodDelete:
		n.deleteObject(w, r, bucket, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *StorageNode) objectPath(bucket, key string) string {
	return filepath.Join(n.DataDir, bucket, key)
}

func (n *StorageNode) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	dir := filepath.Join(n.DataDir, bucket)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		http.Error(w, "failed to create directory", http.StatusInternalServerError)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	filePath := n.objectPath(bucket, key)
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		http.Error(w, "failed to write file", http.StatusInternalServerError)
		return
	}

	n.mu.Lock()
	n.objectCount++
	n.diskUsage += int64(len(data))
	n.storedObjects.Set(float64(n.objectCount))
	n.diskUsageGauge.Set(float64(n.diskUsage))
	n.mu.Unlock()

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, `{"status":"stored","node":"%s"}`, n.ID)
}

func (n *StorageNode) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	filePath := n.objectPath(bucket, key)
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, "read error", http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(data)
}

func (n *StorageNode) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	filePath := n.objectPath(bucket, key)
	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "not found", http.StatusNotFound)
		} else {
			http.Error(w, "stat error", http.StatusInternalServerError)
		}
		return
	}

	size := info.Size()
	if err := os.Remove(filePath); err != nil {
		http.Error(w, "delete error", http.StatusInternalServerError)
		return
	}

	n.mu.Lock()
	n.objectCount--
	n.diskUsage -= size
	n.storedObjects.Set(float64(n.objectCount))
	n.diskUsageGauge.Set(float64(n.diskUsage))
	n.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"deleted"}`)
}

func (n *StorageNode) handleHealth(w http.ResponseWriter, r *http.Request) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       "ok",
		"node_id":      n.ID,
		"object_count": n.objectCount,
		"disk_usage":   n.diskUsage,
	})
}
