package com.p7.framework.redis.timing.task;

import java.io.Serializable;

/**
 * 周期性任务
 */
public class PeriodicTask implements Serializable {

    private static final long serialVersionUID = -8314929951204834039L;

    /**
     * 任务ID
     */
    private String taskId;

    /**
     * 任务执行时间CRON
     */
    private String trigger;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTrigger() {
        return trigger;
    }

    public void setTrigger(String trigger) {
        this.trigger = trigger;
    }
}
