package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	client *redis.Client
}

func NewCache(addr string) *Cache {
	return &Cache{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
	}
}

// SetStreamStatus guarda si el stream esta "RUNNING" O "PAUSED"
func (c *Cache) SetStreamStatus(ctx context.Context, fileID string, status string) error {
	return c.client.Set(ctx, "status:"+fileID, status, 0).Err()
}

// GetStreamStatus consulta el estado actual
func (c *Cache) GetStreamStatus(ctx context.Context, fileID string) (string, error) {
	return c.client.Get(ctx, "status:"+fileID).Result()
}

// SaveLastChunk guarda el progreso para que el Usuario pueda reanudar despues

func (c *Cache) SaveLastChunk(ctx context.Context, fileID string, chunkIndex int64) error {
	return c.client.Set(ctx, "progress:"+fileID, chunkIndex, 0).Err()
}
