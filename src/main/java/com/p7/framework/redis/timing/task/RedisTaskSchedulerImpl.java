package com.p7.framework.redis.timing.task;

import com.p7.framework.redis.timing.task.constant.SchedulerNamespace;
import com.p7.framework.redis.timing.task.constant.StorageRegion;
import com.p7.framework.redis.timing.task.constant.TaskType;
import com.p7.framework.redis.timing.task.redis.RedisOps;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.DefaultTuple;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;

/**
 * 定时任务调度器
 */
public class RedisTaskSchedulerImpl implements RedisTaskScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisTaskSchedulerImpl.class);

    private static final String DEFAULT_SCHEDULER_NAME = SchedulerNamespace.ONE_TASK.getName();

    private StorageRegion scheduler;
    private StorageRegion trigger;
    private RedisOps.RedisZSetOps zSetOps;
    private RedisOps.RedisHashOps hashOps;

    private TaskTriggerListener taskTriggerListener;

    /**
     * 当前时刻无任务时，调度线程轮询周期
     */
    private int pollingDelayMillis = 1000;

    /**
     * 调度器名称空间
     */
    private SchedulerNamespace schedulerNamespace = SchedulerNamespace.ONE_TASK;

    /**
     * 调度器名称/任务分组名称
     */
    private String schedulerName = DEFAULT_SCHEDULER_NAME;

    /**
     * redis连接异常重试次数
     */
    private int maxRetriesOnConnectionFailure = 1;

    /**
     * 调度线程数量
     */
    private int pollingThreadCount = 1;

    /**
     * 周期任务是否使用时间偏移，仅当cron表达式中Seconds和Minutes域为0时有效。
     * 使用时间偏移时，任务原执行时间+当前时间的秒钟与分钟偏移
     */
    private boolean periodicTaskWithShift = false;

    private List<PollingThread> pollingThreads;

    private static ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10000), new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            throw new RejectedExecutionException("Task " + r.toString() + " rejected from " + executor.toString());
        }
    }) {
        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            if (this.getQueue().size() > 10000 * 0.1 && this.getCorePoolSize() < 64) {
                this.setCorePoolSize(this.getCorePoolSize() + 4);
            }
        }
    };

    static {
        executor.allowCoreThreadTimeOut(true);
    }

    /**
     * @param scheduler zset
     * @param trigger   hash
     */
    public RedisTaskSchedulerImpl(StorageRegion scheduler, StorageRegion trigger) {
        this.scheduler = scheduler;
        this.trigger = trigger;
        zSetOps = scheduler.getOperations(RedisOps.RedisZSetOps.class);
        hashOps = trigger.getOperations(RedisOps.RedisHashOps.class);
    }

    public void setSchedulerNamespace(SchedulerNamespace schedulerNamespace) {
        this.schedulerNamespace = schedulerNamespace;
        this.schedulerName = schedulerNamespace.getName();

    }

    public boolean isPeriodicTaskWithShift() {
        return periodicTaskWithShift;
    }

    /**
     * 周期任务是否使用时间偏移，仅当cron表达式中Seconds和Minutes域为0时有效。
     * 使用时间偏移时，任务原执行时间+当前时间的秒钟与分钟偏移
     */
    public void setPeriodicTaskWithShift(boolean periodicTaskWithShift) {
        this.periodicTaskWithShift = periodicTaskWithShift;
    }

    @Override
    public void scheduled(String taskId, Calendar triggerTime) {
        if (schedulerNamespace.getTaskType() != TaskType.ONE_OFF) {
            throw new IllegalStateException("Namespace is not suitable for the task type !");
        }
        if (triggerTime == null) {
            triggerTime = Calendar.getInstance();
        }
        LOGGER.debug("Task trigger time:" + triggerTime.getTimeInMillis());
        zSetOps.add(schedulerName, taskId, triggerTime.getTimeInMillis());
    }

    @Override
    public void scheduled(String taskId, String trigger) {
        if (schedulerNamespace.getTaskType() != TaskType.PERIODIC) {
            throw new IllegalStateException("Namespace is not suitable for the task type !");
        }
        long nextTriggerTime = getNextTriggerTime(trigger);
        if (nextTriggerTime <= 0) {
            throw new IllegalArgumentException("CRON expression is illegal!");
        }
        zSetOps.add(schedulerName, taskId, nextTriggerTime);
        hashOps.put(schedulerName, taskId, trigger);
    }

    @Override
    public Long multiScheduledPeriodicTask(List<PeriodicTask> periodicTasks) {
        if (schedulerNamespace.getTaskType() != TaskType.PERIODIC) {
            throw new IllegalStateException("Namespace is not suitable for the task type !");
        }
        if (periodicTasks == null || periodicTasks.size() > 1000) {
            throw new IllegalArgumentException("Unsuitable number of tasks !");
        }
        Map<String, Object> map = new HashMap<>();
        Set<Tuple> tuples = new HashSet<>();
        for (PeriodicTask periodicTask : periodicTasks) {
            try {
                long nextTriggerTime = getNextTriggerTime(periodicTask.getTrigger());
                if (nextTriggerTime <= 0) {
                    continue;
                }
                Tuple tuple = new DefaultTuple(periodicTask.getTaskId().getBytes(), (double) nextTriggerTime);
                tuples.add(tuple);
            } catch (Exception e) {
                LOGGER.warn("Periodic task(taskId:" + periodicTask.getTaskId() + ") submit failed !", e);
                continue;
            }
            map.put(periodicTask.getTaskId(), periodicTask.getTrigger());
        }
        hashOps.multiPut(schedulerName, map);
        return zSetOps.add(schedulerName, tuples);
    }

    @Override
    public Long multiScheduledOneOffTask(List<OneOffTask> oneOffTasks) {
        if (schedulerNamespace.getTaskType() != TaskType.ONE_OFF) {
            throw new IllegalStateException("Namespace is not suitable for the task type !");
        }
        if (oneOffTasks == null || oneOffTasks.size() > 1000) {
            throw new IllegalArgumentException("Unsuitable number of tasks !");
        }
        Set<Tuple> tuples = new HashSet<>();
        for (OneOffTask oneOffTask : oneOffTasks) {
            if (oneOffTask.getTrigger() == null) {
                oneOffTask.setTrigger(Calendar.getInstance());
            }
            Tuple tuple = new DefaultTuple(oneOffTask.getTaskId().getBytes(), (double) oneOffTask.getTrigger().getTimeInMillis());
            tuples.add(tuple);
        }
        return zSetOps.add(schedulerName, tuples);
    }

    @Override
    public void unscheduled(String taskId) {
        zSetOps.remove(schedulerName, taskId);
        if (schedulerNamespace.getTaskType() == TaskType.PERIODIC) {
            hashOps.delete(schedulerName, taskId);
        }
    }

    @Override
    public void unscheduledAllTasks() {
        zSetOps.delete(schedulerName);
        if (schedulerNamespace.getTaskType() == TaskType.PERIODIC) {
            hashOps.delete(schedulerName);
        }
    }

    @Override
    public boolean hasScheduled(String taskId) {
        return hashOps.hasHashKey(schedulerName, taskId);
    }

    @PostConstruct
    public void initialize() {
        pollingThreads = new ArrayList<>();
        for (int i = 0; i < pollingThreadCount; i++) {
            PollingThread pt = new PollingThread();
            pt.setName(schedulerName + "-polling-" + (i + 1));
            pt.setPriority(Thread.MAX_PRIORITY);
            pt.start();
            pollingThreads.add(pt);
            LOGGER.info(String.format("[%s] Started Redis Scheduler (polling freq: [%sms])", schedulerName, pollingDelayMillis));
        }
    }

    @PreDestroy
    public void destroy() {
        if (pollingThreads != null && pollingThreads.size() > 0) {
            for (PollingThread pollingThread : pollingThreads) {
                pollingThread.requestStop();
            }
        }
    }

    @Override
    public void setTaskTriggerListener(TaskTriggerListener taskTriggerListener) {
        this.taskTriggerListener = taskTriggerListener;
    }

    public void setPollingDelayMillis(int pollingDelayMillis) {
        this.pollingDelayMillis = pollingDelayMillis;
    }

    public void setMaxRetriesOnConnectionFailure(int maxRetriesOnConnectionFailure) {
        this.maxRetriesOnConnectionFailure = maxRetriesOnConnectionFailure;
    }

    public void setPollingThreadCount(int pollingThreadCount) {
        if (pollingThreadCount < 1) {
            return;
        } else if (pollingThreadCount > 10) {
            this.pollingThreadCount = 10;
        } else {
            this.pollingThreadCount = pollingThreadCount;
        }
    }

    private String keyForScheduler() {
        return scheduler.getName() + ":" + schedulerName;
    }

    private String keyForTrigger() {
        return trigger.getName() + ":" + schedulerName;
    }

    private long getNextTriggerTime(String trigger) {
        Calendar now = Calendar.getInstance();
        long nextTime = computeNextTriggerTime(trigger);
        if (nextTime <= 0) {
            return nextTime;
        }
        if (periodicTaskWithShift && trigger.startsWith("0 0 ")) {
            nextTime = nextTime + now.get(Calendar.SECOND) * 1000 + now.get(Calendar.MINUTE) * 60 * 1000;
        }
        return nextTime;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean triggerNextTasksIfFound() {

        return (Boolean) zSetOps.multiExec(new SessionCallback() {
            @Override
            public Object execute(RedisOperations redisOperations) throws DataAccessException {
                boolean taskWasTriggered = false;
                final String key = keyForScheduler();
                redisOperations.watch(key);
                List<String> tasks = findTasksDueForExecution(redisOperations, 1000);
                if (tasks == null || tasks.size() == 0) {
                    redisOperations.unwatch();
                } else {
                    redisOperations.multi();
                    redisOperations.opsForZSet().remove(key, tasks.toArray());
                    boolean executionSuccess = (redisOperations.exec() != null);
                    for (String task : tasks) {
                        if (executionSuccess) {
                            LOGGER.debug(String.format("[%s] Triggering execution of tasks [%s]", schedulerName, task));
                            tryTaskExecution(task);
                            taskWasTriggered = true;
                            if (schedulerNamespace.getTaskType() == TaskType.PERIODIC) {
                                long nextTriggerTime = getNextTriggerTime(String.valueOf(redisOperations.opsForHash().get(keyForTrigger(), task)));
                                if (nextTriggerTime > 0) {
                                    redisOperations.opsForZSet().add(key, task, nextTriggerTime);
                                }
                            }
                        } else {
                            LOGGER.debug(String.format("[%s] Race condition detected for triggering of task [%s]. " + "The task has probably been triggered by another instance of this application.", schedulerName, task));
                        }
                    }
                }
                return taskWasTriggered;
            }
        });
    }

    private void tryTaskExecution(final String task) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    taskTriggerListener.taskTriggered(task);
                } catch (Exception e) {
                    LOGGER.error(String.format("[%s] Error during execution of task [%s]", schedulerName, task), e);
                }
            }
        });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<String> findTasksDueForExecution(RedisOperations ops, final int limit) {
        final long minScore = 0;
        final long maxScore = System.currentTimeMillis();
        Set<byte[]> found = (Set<byte[]>) ops.execute(new RedisCallback() {
            @Override
            public Object doInRedis(RedisConnection redisConnection) throws DataAccessException {
                String key = keyForScheduler();
                return redisConnection.zRangeByScore(key.getBytes(), minScore, maxScore, 0, limit);
            }
        });
        List<String> foundTasks = new ArrayList<>();
        if (found != null && !found.isEmpty()) {
            Iterator<byte[]> it = found.iterator();
            while (it.hasNext()) {
                byte[] valueRaw = it.next();
                Object valueObj = RedisOps.getStringSerializer().deserialize(valueRaw);
                if (valueObj != null) {
                    foundTasks.add(valueObj.toString());
                }
            }
        }
        return foundTasks;
    }

    private class PollingThread extends Thread {
        private boolean stopRequested = false;
        private int numRetriesAttempted = 0;

        public void requestStop() {
            stopRequested = true;
        }

        @Override
        public void run() {
            try {
                while (!stopRequested && !isMaxRetriesAttemptsReached()) {
                    try {
                        attemptTriggerNextTask();
                    } catch (InterruptedException e) {
                        LOGGER.info("PollingThread InterruptedException");
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error(String.format("[%s] Error while polling scheduled tasks. " + "No additional scheduled task will be triggered until the application is restarted.", schedulerName), e);
            }
            if (isMaxRetriesAttemptsReached()) {
                LOGGER.error(String.format("[%s] Maximum number of retries (%s) after Redis connection failure has been reached. " + "No additional scheduled task will be triggered until the application is restarted.", schedulerName, maxRetriesOnConnectionFailure));
            } else {
                LOGGER.info("[%s] Redis Scheduler stopped");
            }
        }

        private void attemptTriggerNextTask() throws InterruptedException {
            try {
                boolean taskTriggered = triggerNextTasksIfFound();
                if (!taskTriggered) {
                    sleep(pollingDelayMillis);
                }
                resetRetriesAttemptsCount();
            } catch (RedisConnectionFailureException e) {
                incrementRetriesAttemptsCount();
                LOGGER.warn(String.format("Connection failure during scheduler polling (attempt %s/%s)", numRetriesAttempted, maxRetriesOnConnectionFailure));
            }
        }

        private boolean isMaxRetriesAttemptsReached() {
            return numRetriesAttempted >= maxRetriesOnConnectionFailure;
        }

        private void resetRetriesAttemptsCount() {
            numRetriesAttempted = 0;
        }

        private void incrementRetriesAttemptsCount() {
            numRetriesAttempted++;
        }
    }

    /**
     * 根据CRON表达式计算下次触发时间
     *
     * @param cronExpression
     * @return -1-非法的CRON表达式，0-当前时间之后不再可能触发的表达式
     */
    public static long computeNextTriggerTime(String cronExpression) {
        try {
            CronExpression expression = new CronExpression(cronExpression);
            Date nextTime = expression.getNextValidTimeAfter(new Date());
            return nextTime == null ? 0 : nextTime.getTime();
        } catch (ParseException e) {
            LOGGER.error("cron expression({}) parse error!", cronExpression);
            return -1;
        }
    }

}
