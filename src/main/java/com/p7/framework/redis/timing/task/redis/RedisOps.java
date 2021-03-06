package com.p7.framework.redis.timing.task.redis;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.p7.framework.redis.timing.task.constant.SerializeFormat;
import com.p7.framework.redis.timing.task.constant.StorageRegion;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.*;

/**
 * redis
 */
public class RedisOps {

    private RedisOps() {
    }

    StorageRegion sr;

    static RedisSerializer<String> stringSerializer = new StringRedisSerializer();

    static RedisSerializer<Object> blobSerializer = new JdkSerializationRedisSerializer();

    private static enum Singleton {
        INSTANCE;

        private RedisHashOps redisHashOps;
        private RedisZSetOps redisZSetOps;

        private Singleton() {
            redisHashOps = new RedisHashOps();
            redisZSetOps = new RedisZSetOps();
        }

        public RedisHashOps getRedisHashOps() {
            return redisHashOps;
        }

        public RedisZSetOps getRedisZSetOps() {
            return redisZSetOps;
        }

    }

    public static RedisSerializer<String> getStringSerializer() {
        return stringSerializer;
    }

    String keyConvert(String key) {
        if (this.sr != null) {
            if (key == null || key.trim().equals("")) {
                throw new IllegalArgumentException();
            } else {
                return sr.getName() + ":" + key;
            }
        } else {
            return key;
        }
    }

    <T> Jackson2JsonRedisSerializer<T> configuredJackson2JsonRedisSerializer(Class<T> clazz) {
        Jackson2JsonRedisSerializer<T> serializer = new Jackson2JsonRedisSerializer<T>(clazz);
        ObjectMapper objectMapper = new ObjectMapper();
        //json???????????????????????????
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        //?????????json??????null
        objectMapper.setSerializationInclusion(Include.NON_NULL);
        serializer.setObjectMapper(objectMapper);
        return serializer;
    }

    byte[] serialize(Object object) {
        return serialize(object, SerializeFormat.STRING);
    }

    byte[] serialize(Object object, SerializeFormat sf) {
        if (object == null) {
            return new byte[0];
        }
        if (sf == SerializeFormat.BLOB) {
            return blobSerializer.serialize(object);
        }
        if (object instanceof String || CacheKeyGenerator.isPrimitive(object.getClass())) {
            return stringSerializer.serialize(String.valueOf(object));
        } else {
            return configuredJackson2JsonRedisSerializer(object.getClass()).serialize(object);
        }
    }

    /**
     * ??????
     * <p>
     * ???????????????O(1)
     *
     * @param key
     */
    public void delete(final String key) {
        RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
            @Override
            public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                redisTemplate.delete(keyConvert(key));
                return null;
            }
        });
    }

    /**
     * ?????????????????????????????????
     *
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T> T multiExec(final SessionCallback<T> session) {
        return (T) RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
            @Override
            public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                return redisTemplate.execute(session);
            }
        });
    }

    /**
     * hash?????????????????????
     */
    public static class RedisHashOps extends RedisOps {

        private RedisHashOps() {
        }

        /**
         * ?????????????????????
         *
         * @param
         */
        RedisHashOps(StorageRegion storageRegion) {
            this.sr = storageRegion;
        }

        /**
         * ??????????????????
         *
         * @return
         */
        static RedisHashOps getInstance() {
            return Singleton.INSTANCE.getRedisHashOps();
        }

        /**
         * ??????hash??????????????????
         * <p>
         * ???????????????O(N)??? N???????????????hashKeys?????????
         *
         * @param key
         * @param hashKeys
         */
        public void delete(final String key, final String... hashKeys) {
            RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
                @Override
                public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                    return redisTemplate.execute(new RedisCallback<Void>() {
                        @Override
                        public Void doInRedis(RedisConnection connection) throws DataAccessException {
                            final byte[][] rawHashKeys = new byte[hashKeys.length][];
                            int counter = 0;
                            for (String hashKey : hashKeys) {
                                rawHashKeys[counter++] = serialize(hashKey);
                            }
                            connection.hDel(serialize(keyConvert(key)), rawHashKeys);
                            return null;
                        }
                    });
                }
            });
        }

        /**
         * hashKey??????
         * <p>
         * ???????????????O(1)
         *
         * @param key
         * @param hashKey
         * @return
         */
        public Boolean hasHashKey(final String key, final String hashKey) {
            return (Boolean) RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
                @Override
                public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                    return redisTemplate.execute(new RedisCallback<Boolean>() {
                        @Override
                        public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                            return connection.hExists(serialize(keyConvert(key)), serialize(hashKey));
                        }
                    });
                }
            });
        }

        /**
         * ????????????????????????????????????????????????????????????
         * <p>
         * ???????????????O(1)
         *
         * @param key
         * @param hashKey
         * @param value
         */
        public void put(final String key, final String hashKey, final Object value) {
            RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
                @Override
                public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                    return redisTemplate.execute(new RedisCallback<Void>() {
                        @Override
                        public Void doInRedis(RedisConnection connection) throws DataAccessException {
                            byte[] k = serialize(keyConvert(key));
                            connection.hSet(k, serialize(hashKey), serialize(value));
                            if (sr != null && sr.getExpire() > 0) {
                                connection.expire(k, sr.getExpire());
                            }
                            return null;
                        }
                    });
                }
            });
        }

        /**
         * ??????????????? hashkey-value (???-???)????????????????????? key???,?????????????????????????????????????????????
         * <p>
         * ???????????????O(N)???N???map??????????????????
         *
         * @param key
         * @param map
         */
        public void multiPut(final String key, final Map<String, Object> map) {
            RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
                @Override
                public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                    return redisTemplate.execute(new RedisCallback<Void>() {
                        @Override
                        public Void doInRedis(RedisConnection connection) throws DataAccessException {
                            byte[] k = serialize(keyConvert(key));
                            Map<byte[], byte[]> hashes = new LinkedHashMap<byte[], byte[]>(map.size());
                            for (Map.Entry<String, Object> entry : map.entrySet()) {
                                hashes.put(serialize(entry.getKey()), serialize(entry.getValue()));
                            }
                            connection.hMSet(k, hashes);
                            if (sr != null && sr.getExpire() > 0) {
                                connection.expire(k, sr.getExpire());
                            }
                            return null;
                        }
                    });
                }
            });
        }

    }

    /**
     * zSet?????????????????????
     */
    public static class RedisZSetOps extends RedisOps {
        private RedisZSetOps() {
        }

        /**
         * ?????????????????????
         *
         * @param
         */
        RedisZSetOps(StorageRegion storageRegion) {
            this.sr = storageRegion;
        }

        /**
         * ??????????????????
         *
         * @return
         */
        static RedisZSetOps getInstance() {
            return Singleton.INSTANCE.getRedisZSetOps();
        }

        /**
         * ????????? member???????????? score????????????????????? key???????????????member??????????????????????????????????????????
         * ??????member???score?????????????????????????????????member?????????????????????member????????????????????????
         * ???????????????????????????????????????????????????
         * <p>
         * ???????????????O(log(N))???N??????????????????????????????
         *
         * @param key
         * @param member
         * @param score
         */
        public Boolean add(final String key, final Object member, final double score) {
            return (Boolean) RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
                @Override
                public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                    return redisTemplate.execute(new RedisCallback<Boolean>() {
                        @Override
                        public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                            byte[] k = serialize(keyConvert(key));
                            Boolean res = connection.zAdd(k, score, serialize(member));
                            if (sr != null && sr.getExpire() > 0) {
                                connection.expire(k, sr.getExpire());
                            }
                            return res;
                        }
                    });
                }
            });
        }

        /**
         * ?????????????????? member ???????????? score ?????????????????? member ???????????????????????????????????????????????? member???
         * score????????????????????????????????? member ????????????????????? member ????????????????????????
         * ???????????????????????????????????????????????????
         * <p>
         * ???????????????O(M*log(N))??? N ???????????????????????? M ????????????????????????????????????
         *
         * @param key
         * @param tuples
         * @return ??????????????????????????????????????????????????????????????????????????????????????????
         */
        public Long add(final String key, final Set<Tuple> tuples) {
            return (Long) RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
                @Override
                public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                    return redisTemplate.execute(new RedisCallback<Long>() {
                        @Override
                        public Long doInRedis(RedisConnection connection) throws DataAccessException {
                            byte[] k = serialize(keyConvert(key));
                            Long res = connection.zAdd(k, tuples);
                            if (sr != null && sr.getExpire() > 0) {
                                connection.expire(k, sr.getExpire());
                            }
                            return res;
                        }
                    });
                }
            });
        }

        /**
         * ??????????????? key ????????????????????????????????????????????????????????????
         * <p>
         * ???????????????O(M*log(N))???N????????????????????????????????? M?????????????????????????????????
         *
         * @param key
         * @param members
         * @return ?????????????????????????????????
         */
        public Long remove(final String key, final Object... members) {
            return (Long) RedisClientFlowControl.execute(new RedisClientFlowControl.Statement() {
                @Override
                public Object prepare(final RedisTemplate<String, Object> redisTemplate) {
                    return redisTemplate.execute(new RedisCallback<Long>() {
                        @Override
                        public Long doInRedis(RedisConnection connection) throws DataAccessException {
                            byte[][] rawValues = new byte[members.length][];
                            int i = 0;
                            for (Object value : members) {
                                rawValues[i++] = serialize(value);
                            }
                            return connection.zRem(serialize(keyConvert(key)), rawValues);
                        }
                    });
                }
            });
        }
    }
}
