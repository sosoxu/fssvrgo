package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sosoxu/fssvrgo/internal/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ServiceInstance struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Address  string            `json:"address"`
	Port     int               `json:"port"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type ServiceDiscovery struct {
	client   *clientv3.Client
	prefix   string
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewServiceDiscovery(etcdEndpoints []string, prefix string, intervalSeconds int) (*ServiceDiscovery, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd for discovery: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ServiceDiscovery{
		client:   cli,
		prefix:   prefix + "/services/",
		interval: time.Duration(intervalSeconds) * time.Second,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (d *ServiceDiscovery) Close() error {
	d.cancel()
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}

func (d *ServiceDiscovery) Register(instance *ServiceInstance) error {
	data, err := json.Marshal(instance)
	if err != nil {
		return fmt.Errorf("failed to marshal service instance: %w", err)
	}

	key := d.prefix + instance.Name + "/" + instance.ID
	lease, err := d.client.Grant(d.ctx, int64(d.interval.Seconds())*3)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	_, err = d.client.Put(d.ctx, key, string(data), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	// Start keepalive
	keepAliveCh, err := d.client.KeepAlive(d.ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to start keepalive: %w", err)
	}

	// Consume keepalive responses
	go func() {
		for {
			select {
			case <-d.ctx.Done():
				return
			case resp := <-keepAliveCh:
				if resp == nil {
					logger.Error("Service discovery keepalive expired for %s", instance.ID)
					return
				}
			}
		}
	}()

	logger.Info("Service registered: %s (%s:%d)", instance.Name, instance.Address, instance.Port)
	return nil
}

func (d *ServiceDiscovery) Deregister(instance *ServiceInstance) error {
	key := d.prefix + instance.Name + "/" + instance.ID
	_, err := d.client.Delete(d.ctx, key)
	if err != nil {
		return fmt.Errorf("failed to deregister service: %w", err)
	}
	logger.Info("Service deregistered: %s (%s)", instance.Name, instance.ID)
	return nil
}

func (d *ServiceDiscovery) Discover(serviceName string) ([]*ServiceInstance, error) {
	prefix := d.prefix + serviceName + "/"
	ctx, cancel := context.WithTimeout(d.ctx, 5*time.Second)
	defer cancel()

	resp, err := d.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to discover service %s: %w", serviceName, err)
	}

	var instances []*ServiceInstance
	for _, kv := range resp.Kvs {
		var instance ServiceInstance
		if err := json.Unmarshal(kv.Value, &instance); err != nil {
			continue
		}
		instances = append(instances, &instance)
	}
	return instances, nil
}

func (d *ServiceDiscovery) Watch(serviceName string) (<-chan []*ServiceInstance, error) {
	prefix := d.prefix + serviceName + "/"
	watchCh := d.client.Watch(d.ctx, prefix, clientv3.WithPrefix())

	resultCh := make(chan []*ServiceInstance, 10)

	go func() {
		defer close(resultCh)
		for {
			select {
			case <-d.ctx.Done():
				return
			case watchResp := <-watchCh:
				if watchResp.Err() != nil {
					continue
				}

				instances, err := d.Discover(serviceName)
				if err != nil {
					continue
				}
				select {
				case resultCh <- instances:
				default:
				}
			}
		}
	}()

	return resultCh, nil
}
