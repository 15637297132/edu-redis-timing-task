package com.p7.framework.redis.timing.task.client;

import com.p7.framework.redis.timing.task.TaskTriggerListener;

/**
 * 周期任务回调
 */
public class CycleTaskTriggerListener implements TaskTriggerListener {

    @Override
    public void taskTriggered(String taskId) {
        System.out.println("####################################");
        System.out.println("CycleTaskTriggerListener , taskId is " + taskId);
        System.out.println("####################################");
    }

}
