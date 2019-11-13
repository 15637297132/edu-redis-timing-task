package com.p7.framework.redis.timing.task.redis;

import com.p7.framework.redis.timing.task.constant.StorageRegion;

/**
 * Redis的数据结构
 */
public enum DataStructure {
    HASH {
        @Override
        RedisOps getSharedRedisOps() {
            return RedisOps.RedisHashOps.getInstance();
        }

        @Override
        public RedisOps getRedisOps(StorageRegion storageRegion) {
            return new RedisOps.RedisHashOps(storageRegion);
        }
    }, ZSET {
        @Override
        RedisOps getSharedRedisOps() {
            return RedisOps.RedisZSetOps.getInstance();
        }

        @Override
        public RedisOps getRedisOps(StorageRegion storageRegion) {
            return new RedisOps.RedisZSetOps(storageRegion);
        }
    };

    abstract RedisOps getSharedRedisOps();

    public abstract RedisOps getRedisOps(StorageRegion storageRegion);

    public RedisOps getRedisOps() {
        return getSharedRedisOps();
    }

}
