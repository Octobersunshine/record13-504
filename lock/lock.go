package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type DistributedLock struct {
	client *redis.Client
}

func NewDistributedLock(client *redis.Client) *DistributedLock {
	return &DistributedLock{client: client}
}

type Lock struct {
	client *redis.Client
	key    string
	value  string
}

var unlockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
else
	return 0
end
`)

var refreshScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("pexpire", KEYS[1], ARGV[2])
else
	return 0
end
`)

func (d *DistributedLock) Acquire(ctx context.Context, key string, ttl time.Duration) (*Lock, bool, error) {
	value := uuid.New().String()
	ok, err := d.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return nil, false, fmt.Errorf("acquire lock failed: %w", err)
	}
	if !ok {
		return nil, false, nil
	}
	return &Lock{
		client: d.client,
		key:    key,
		value:  value,
	}, true, nil
}

func (l *Lock) Release(ctx context.Context) error {
	result, err := unlockScript.Run(ctx, l.client, []string{l.key}, l.value).Result()
	if err != nil {
		return fmt.Errorf("release lock failed: %w", err)
	}
	if result.(int64) == 0 {
		return fmt.Errorf("release lock: lock not owned or already released")
	}
	return nil
}

func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) (bool, error) {
	result, err := refreshScript.Run(ctx, l.client, []string{l.key}, l.value, ttl.Milliseconds()).Result()
	if err != nil {
		return false, fmt.Errorf("refresh lock failed: %w", err)
	}
	return result.(int64) == 1, nil
}

func (l *Lock) Key() string {
	return l.key
}

func (d *DistributedLock) ForceRelease(ctx context.Context, key string) error {
	deleted, err := d.client.Del(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("force release lock failed: %w", err)
	}
	if deleted == 0 {
		return fmt.Errorf("force release lock: key %s does not exist", key)
	}
	return nil
}
