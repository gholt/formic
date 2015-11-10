package main

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

type FileService interface {
	GetChunk(id []byte) ([]byte, error)
	WriteChunk(id, data []byte) error
}

func newRedisPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
	}
}

type OortFS struct {
	rpool *redis.Pool
}

func NewOortFS(host string) *OortFS {
	return &OortFS{
		rpool: newRedisPool(host),
	}
}

func (o *OortFS) GetChunk(id []byte) ([]byte, error) {
	rc := o.rpool.Get()
	defer rc.Close()
	data, err := redis.Bytes(rc.Do("GET", id))
	if err != nil {
		if err == redis.ErrNil {
			// file is empty or doesn't exist
			return []byte(""), nil
		}
		return []byte(""), err
	}
	return data, nil
}

func (o *OortFS) WriteChunk(id, data []byte) error {
	rc := o.rpool.Get()
	defer rc.Close()
	_, err := rc.Do("Set", id, data)
	return err
}
