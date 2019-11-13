package com.p7.framework.redis.timing.task;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Calendar;
import java.util.Date;

/**
 * @author Yangzhen
 * @Description
 * @date 2019-08-24 14:53
 **/
public class Main {

    public static void main(String[] args) {

        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:/spring/spring-timing-task.xml");
        context.start();
        RedisTaskScheduler oneScheduler = context.getBean("oneScheduler", RedisTaskScheduler.class);
        Date time = new Date();

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        oneScheduler.scheduled("2-2", calendar);

        RedisTaskScheduler cycleScheduler = context.getBean("cycleScheduler", RedisTaskScheduler.class);
        cycleScheduler.scheduled("cycle", "0/10 * * * * ?");

        synchronized (Main.class) {
            try {
                Main.class.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
