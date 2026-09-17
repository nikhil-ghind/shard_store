package metadata

import (
	"fmt"
	"sync"
	"time"
)

type ObjectMeta struct {
	Key       string    `json:"key"`
	Bucket    string    `json:"bucket"`
	Size      int64     `json:"size"`
	Checksum  string    `json:"checksum"`
	Replicas  []string  `json:"replicas"`
	CreatedAt time.Time `json:"created_at"`
}

type MetadataStore struct {
	mu      sync.RWMutex
	buckets map[string]bool
	objects map[string]map[string]*ObjectMeta // bucket -> key -> meta
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{
		buckets: make(map[string]bool),
		objects: make(map[string]map[string]*ObjectMeta),
	}
}

func (s *MetadataStore) CreateBucket(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets[name] {
		return fmt.Errorf("bucket %q already exists", name)
	}
	s.buckets[name] = true
	s.objects[name] = make(map[string]*ObjectMeta)
	return nil
}

func (s *MetadataStore) DeleteBucket(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.buckets[name] {
		return fmt.Errorf("bucket %q not found", name)
	}
	if len(s.objects[name]) > 0 {
		return fmt.Errorf("bucket %q is not empty", name)
	}
	delete(s.buckets, name)
	delete(s.objects, name)
	return nil
}

func (s *MetadataStore) ListBuckets() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, 0, len(s.buckets))
	for b := range s.buckets {
		result = append(result, b)
	}
	return result
}

func (s *MetadataStore) BucketExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.buckets[name]
}

func (s *MetadataStore) PutObject(meta *ObjectMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.buckets[meta.Bucket] {
		return fmt.Errorf("bucket %q not found", meta.Bucket)
	}
	s.objects[meta.Bucket][meta.Key] = meta
	return nil
}

func (s *MetadataStore) GetObject(bucket, key string) (*ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.buckets[bucket] {
		return nil, fmt.Errorf("bucket %q not found", bucket)
	}
	obj, ok := s.objects[bucket][key]
	if !ok {
		return nil, fmt.Errorf("object %q not found in bucket %q", key, bucket)
	}
	return obj, nil
}

func (s *MetadataStore) DeleteObject(bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.buckets[bucket] {
		return fmt.Errorf("bucket %q not found", bucket)
	}
	if _, ok := s.objects[bucket][key]; !ok {
		return fmt.Errorf("object %q not found in bucket %q", key, bucket)
	}
	delete(s.objects[bucket], key)
	return nil
}

func (s *MetadataStore) ListObjects(bucket string) ([]*ObjectMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.buckets[bucket] {
		return nil, fmt.Errorf("bucket %q not found", bucket)
	}
	result := make([]*ObjectMeta, 0, len(s.objects[bucket]))
	for _, obj := range s.objects[bucket] {
		result = append(result, obj)
	}
	return result, nil
}

// GetAllObjects returns all objects across all buckets.
func (s *MetadataStore) GetAllObjects() []*ObjectMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*ObjectMeta
	for _, objs := range s.objects {
		for _, obj := range objs {
			result = append(result, obj)
		}
	}
	return result
}

// UpdateReplicas replaces the replica list for an object.
func (s *MetadataStore) UpdateReplicas(bucket, key string, replicas []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.buckets[bucket] {
		return fmt.Errorf("bucket %q not found", bucket)
	}
	obj, ok := s.objects[bucket][key]
	if !ok {
		return fmt.Errorf("object %q not found", key)
	}
	obj.Replicas = replicas
	return nil
}
