package com.p7.framework.redis.timing.task.constant;


import com.p7.framework.redis.timing.task.redis.DataStructure;

/**
 * 此操作接口只有在spring容器初始化之后方可调用，若需要在服务启动时通过此接口访问redis,
 * 请实现ApplicationListener，监听ContextRefreshedEvent事件发生时调用.
 */
public enum StorageRegion {


    REDIS_SCHEDULER("redis_scheduler", DataStructure.ZSET, -1),
    REDIS_TRIGGER("redis_trigger", DataStructure.HASH, -1),

    ;

    public <T> T getOperations(Class<T> clazz) {
        return (T) getDataStructure().getRedisOps(this);
    }

    /**
     * key前缀，唯一性
     */
    private final String name;
    /**
     * 数据存储的数据结构
     */
    private final DataStructure dataStructure;
    /**
     * 过期时间（秒），-1永不过期
     */
    private final int expire;


    private StorageRegion(String name, DataStructure dataStructure, int expire) {

        this.name = name;
        this.dataStructure = dataStructure;
        this.expire = expire;
    }

    public String getName() {
        return this.name;
    }

    public DataStructure getDataStructure() {
        return dataStructure;
    }

    public int getExpire() {
        return expire;
    }

}
