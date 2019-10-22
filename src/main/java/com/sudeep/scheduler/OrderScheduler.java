package com.sudeep.scheduler;

import com.alibaba.fastjson.JSON;
import com.sudeep.task.OrderTask;
import com.sudeep.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.*;

public class OrderScheduler {
    private static final Logger logger = LoggerFactory.getLogger("OrderScheduler");

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    static class TaskFuturePair {
        final OrderTask orderTask;
        final ScheduledFuture scheduledFuture;

        TaskFuturePair(OrderTask orderTask, ScheduledFuture scheduledFuture) {
            this.orderTask = orderTask;
            this.scheduledFuture = scheduledFuture;
        }
    }

    private final Random random = new Random(Calendar.getInstance().getTimeInMillis());

    private void randomBias(Calendar calendar) {
        calendar.add(Calendar.SECOND, random.nextInt(30) - 15);
    }

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, TaskFuturePair>> groups = new ConcurrentHashMap<>();

    public ScheduledFuture schedule(OrderTask orderTask, Calendar calendar) {
        randomBias(calendar);
        logger.info("[OrderScheduler.schedule." + orderTask.getId() + "] Time: " + DateUtil.calendarToString(calendar, DateUtil.datetimeFormat));
        logger.info("[OrderScheduler.schedule." + orderTask.getId() + "] Order: " + JSON.toJSONString(orderTask.getOrder()));

        orderTask.setTimeToSend(calendar);

        ScheduledFuture future = schedule(orderTask, calendar.getTime());

        String groupId = orderTask.getOrderToSend().getGroupId();
        String otsId = orderTask.getId();
        ConcurrentHashMap<String, TaskFuturePair> group = groups.get(groupId);
        if (group == null)
            group = new ConcurrentHashMap<>();
        TaskFuturePair tfp = new TaskFuturePair(orderTask, future);
        group.put(otsId, tfp);

        groups.put(groupId, group);
        return future;
    }

    private ScheduledFuture schedule(Runnable task, Date startTime) {
        long initialDelay = startTime.getTime() - System.currentTimeMillis();
        try {
            return executor.schedule(task, initialDelay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            throw new RejectedExecutionException("Executor [" + executor + "] did not accept task: " + task, ex);
        }
    }
}
