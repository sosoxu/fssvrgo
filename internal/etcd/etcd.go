package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/sosoxu/fssvrgo/internal/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdManager struct {
	client *clientv3.Client
	prefix string
	ctx    context.Context
	cancel context.CancelFunc
}

func NewEtcdManager(endpoints []string, prefix string) (*EtcdManager, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	mgr := &EtcdManager{
		client: cli,
		prefix: prefix,
		ctx:    ctx,
		cancel: cancel,
	}

	logger.Info("Etcd connected to %v with prefix '%s'", endpoints, prefix)
	return mgr, nil
}

func (m *EtcdManager) Close() error {
	m.cancel()
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}

func (m *EtcdManager) GetClient() *clientv3.Client {
	return m.client
}

func (m *EtcdManager) GetPrefix() string {
	return m.prefix
}

func (m *EtcdManager) Put(key, value string) error {
	fullKey := m.prefix + key
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	_, err := m.client.Put(ctx, fullKey, value)
	if err != nil {
		return fmt.Errorf("failed to put key %s: %w", fullKey, err)
	}
	return nil
}

func (m *EtcdManager) Get(key string) (string, error) {
	fullKey := m.prefix + key
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	resp, err := m.client.Get(ctx, fullKey)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", fullKey, err)
	}

	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("key %s not found", fullKey)
	}
	return string(resp.Kvs[0].Value), nil
}

func (m *EtcdManager) Delete(key string) error {
	fullKey := m.prefix + key
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	_, err := m.client.Delete(ctx, fullKey)
	if err != nil {
		return fmt.Errorf("failed to delete key %s: %w", fullKey, err)
	}
	return nil
}

func (m *EtcdManager) List(prefix string) (map[string]string, error) {
	fullPrefix := m.prefix + prefix
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	resp, err := m.client.Get(ctx, fullPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list prefix %s: %w", fullPrefix, err)
	}

	result := make(map[string]string)
	for _, kv := range resp.Kvs {
		shortKey := string(kv.Key)[len(m.prefix):]
		result[shortKey] = string(kv.Value)
	}
	return result, nil
}

func (m *EtcdManager) Watch(key string) clientv3.WatchChan {
	fullKey := m.prefix + key
	return m.client.Watch(m.ctx, fullKey, clientv3.WithPrefix())
}

func (m *EtcdManager) KeepAlive(key, value string, ttl int64) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer cancel()

	lease, err := m.client.Grant(ctx, ttl)
	if err != nil {
		return nil, fmt.Errorf("failed to create lease: %w", err)
	}

	fullKey := m.prefix + key
	putCtx, putCancel := context.WithTimeout(m.ctx, 5*time.Second)
	defer putCancel()

	_, err = m.client.Put(putCtx, fullKey, value, clientv3.WithLease(lease.ID))
	if err != nil {
		return nil, fmt.Errorf("failed to put key with lease: %w", err)
	}

	keepAliveCh, err := m.client.KeepAlive(m.ctx, lease.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to start keepalive: %w", err)
	}

	return keepAliveCh, nil
}
