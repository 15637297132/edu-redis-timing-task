package com.p7.framework.redis.timing.task.constant;

/**
 * 调度器命名空间
 */
public enum SchedulerNamespace {
	
	CYCLE_TASK("cycle_task", TaskType.PERIODIC),
	ONE_TASK("one_task", TaskType.ONE_OFF),
	;
	
	private final String name;
	private final TaskType taskType;
	
	private SchedulerNamespace( String name, TaskType taskType){
		this.name = name;
		this.taskType = taskType;
	}
	public String getName(){
		return this.name;
	}
	public TaskType getTaskType() {
		return taskType;
	}

}
