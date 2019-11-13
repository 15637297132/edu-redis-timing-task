package com.p7.framework.redis.timing.task;

/**
 * 任务回调接口
 */
public interface TaskTriggerListener {

    /**
     * 回调方法
     *
     * @param taskId
     */
    void taskTriggered(String taskId);
}
