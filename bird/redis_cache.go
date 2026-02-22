package bird

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisCache struct {
	client    *redis.Client
	keyPrefix string
}

func NewRedisCache(config CacheConfig) (*RedisCache, error) {

	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisServer,
		Password: config.RedisPassword,
		DB:       config.RedisDb,
	})

	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	cache := &RedisCache{
		client: client,
	}

	return cache, nil
}

// Get retrievs a birdwatcher `Parsed` result from
// the redis cache.
func (rc *RedisCache) Get(key string) (Parsed, error) {
	ctx := context.Background()
	key = rc.keyPrefix + key //"B" + IPVersion + "_" + key
	data, err := rc.client.Get(ctx, key).Result()
	if err != nil {
		return NilParse, err
	}

	parsed := Parsed{}
	if err = json.Unmarshal([]byte(data), &parsed); err != nil {
		return NilParse, err
	}

	ttl, err := parseCacheTTL(parsed["ttl"])
	if err != nil {
		return NilParse, fmt.Errorf("invalid TTL value for key: %s", key)
	}
	// Deal with the inband TTL if present
	if !ttl.Equal(time.Time{}) && ttl.Before(time.Now()) {
		return NilParse, err // TTL expired
	}

	return parsed, err // cache hit
}

// Set adds a birdwatcher `Parsed` result
// to the redis cache.
func (rc *RedisCache) Set(key string, parsed Parsed, ttl int) error {
	switch {
	case ttl == 0:
		return nil // do not cache

	case ttl > 0:
		key = rc.keyPrefix + key //TODO "B" + IPVersion + "_" + key
		payload, err := json.Marshal(parsed)
		if err != nil {
			return err
		}

		ctx := context.Background()
		_, err = rc.client.Set(
			ctx, key, payload, time.Duration(ttl)*time.Minute).Result()
		return err

	default: // ttl negative - invalid
		return fmt.Errorf("negative TTL value for key: %s", key)
	}
}

func (rc *RedisCache) Expire() int {
	log.Printf("Cannot expire entries in RedisCache backend, redis does this automatically")
	return 0
}

// Helperfunction to decode the cache ttl stored
// in the cache - which will most likely just be
// RFC3339 timestamp.
func parseCacheTTL(cacheTTL interface{}) (time.Time, error) {
	if cacheTTL == nil {
		// We preseve the nil value as a zero value
		return time.Time{}, nil
	}

	switch v := cacheTTL.(type) {
	case string:
		ttl, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return time.Time{}, err
		}
		return ttl, nil
	case time.Time:
		return v, nil
	}
	return time.Time{}, nil
}
