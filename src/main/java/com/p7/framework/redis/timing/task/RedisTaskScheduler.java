package com.p7.framework.redis.timing.task;

import java.util.Calendar;
import java.util.List;

/**
 * 定时任务调度器接口
 */
public interface RedisTaskScheduler {

    /**
     * 设置任务回调
     * @param taskTriggerListener
     */
    void setTaskTriggerListener(TaskTriggerListener taskTriggerListener);

    /**
     * 提交一个任务（一次性任务）
     * @param taskId
     * @param trigger 任务执行时间，为null或者过去的时间时立即执行
     */
    void scheduled(String taskId, Calendar trigger);

    /**
     * 提交一个任务（周期性任务）
     * 任务执行的CRON表达式非法或者当前时间之后不再执行的CRON表达式无法提交
     * @param taskId
     * @param trigger CRON表达式
     */
    void scheduled(String taskId, String trigger);


    /**
     * 批量提交周期性任务（一次至多1000个）
     * 自动忽略非法的CRON表达式任务和当前时间之后不再执行的CRON表达式任务
     * @param periodicTasks
     * @return 提交成功的任务数
     */
    Long multiScheduledPeriodicTask(List<PeriodicTask> periodicTasks);

    /**
     * 批量提交一次性任务（一次至多1000个）
     * @param oneOffTasks
     * @return 提交成功的任务数
     */
    Long multiScheduledOneOffTask(List<OneOffTask> oneOffTasks);

    /**
     * 删除当前调度器内所有任务
     */
    void unscheduledAllTasks();

    /**
     * 删除一个任务
     * @param taskId
     */
    void unscheduled(String taskId);

    /**
     * 任务是否已存在
     * @param taskId
     * @return
     */
    boolean hasScheduled(String taskId);
}
