package com.p7.framework.redis.timing.task.redis;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.Semaphore;

@Component
public final class RedisClientFlowControl {
	
	private static RedisTemplate<String, Object> redisTemplate;
	
	private static Semaphore semp;
	
	static RedisTemplate<String, Object> getRedisTemplate() {
		try {
			semp.acquire();
			return redisTemplate;
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}	
	}
	
	@Resource(name = "redisTemplate")
	void setRedisTemplate(RedisTemplate<String, Object> redisTemplate) {
		RedisClientFlowControl.redisTemplate = redisTemplate;
	}
	
	@Value("20")
	void setSemp(Integer permits) {
		RedisClientFlowControl.semp = new Semaphore(permits);
	}

	static void release(){
		semp.release();
	}
	
	static Object execute(Statement stmt){
		RedisTemplate<String, Object> redisTemplate = getRedisTemplate();
		try {
			return stmt.prepare(redisTemplate);
		} finally{
			release();
		}
	}
	
	interface Statement{
		Object prepare(final RedisTemplate<String, Object> redisTemplate);
	}

}
