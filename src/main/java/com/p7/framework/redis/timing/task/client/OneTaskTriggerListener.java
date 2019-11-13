package com.p7.framework.redis.timing.task.client;

import com.p7.framework.redis.timing.task.TaskTriggerListener;

/**
 * 一次性任务回调
 */
public class OneTaskTriggerListener implements TaskTriggerListener {

    @Override
    public void taskTriggered(String taskId) {

        System.out.println("####################################");
        System.out.println("OneTaskTriggerListener , taskId is " + taskId);
        System.out.println("####################################");
    }

}
