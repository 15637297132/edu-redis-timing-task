package com.p7.framework.redis.timing.task;

import java.io.Serializable;
import java.util.Calendar;

/**
 * 一次性任务
 */
public class OneOffTask implements Serializable {

	private static final long serialVersionUID = -528221414797144695L;
	
	/**
	 * 任务ID
	 */
	private String taskId;
	
	/**
	 * 任务执行时间
	 */
	private Calendar trigger;

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public Calendar getTrigger() {
		return trigger;
	}

	public void setTrigger(Calendar trigger) {
		this.trigger = trigger;
	}
}
