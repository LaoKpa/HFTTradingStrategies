package com.sudeep.service;

import com.sudeep.dao.OrderBlotterDao;
import com.sudeep.domain.Entity.OrderBlotter;
import com.sudeep.scheduler.OrderScheduler;
import com.sudeep.task.OrderTask;
import com.sudeep.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Calendar;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

public class MinimizeSlippageManagerService extends TimerTask {
    private static final Logger log = LoggerFactory.getLogger(MinimizeSlippageManagerService.class);

    private final List<OrderBlotter> simulatedOrderBlotterByInterval;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, OrderScheduler.TaskFuturePair>> schedulerOrdersByGroupId;
    private final OrderBlotterDao orderBlotterDao;
    private final String groupId;
    private final int intervalMinute;

    public MinimizeSlippageManagerService(List<OrderBlotter> simulatedOrderBlotterByInterval, ConcurrentHashMap<String, ConcurrentHashMap<String, OrderScheduler.TaskFuturePair>> schedulerOrdersByGroupId, OrderBlotterDao orderBlotterDao, String groupId, int intervalMinute) {
        this.simulatedOrderBlotterByInterval = simulatedOrderBlotterByInterval;
        this.schedulerOrdersByGroupId = schedulerOrdersByGroupId;
        this.orderBlotterDao = orderBlotterDao;
        this.groupId = groupId;
        this.intervalMinute = intervalMinute;
    }

    @Override
    public void run() {
        updateScheduledOrdersBasedOnVolumeProfile();
    }

    private void updateScheduledOrdersBasedOnVolumeProfile() {
        if (log.isTraceEnabled()) {
            log.trace("Re-Balance the scheduled orders based on the change of volume profile compared to the previous volume profile used to split the orders!");
        }

        final Calendar simulateCurrentTime = DateUtil.getSimulatedCurrentTimeForTomorrow();
        final List<OrderBlotter> orderBlotterToCompare = getPreviousOrderBlottersForTimeInterval();
        final List<OrderBlotter> todayOrderBlotter = getSimulatedOrderBlotterByInterval(simulateCurrentTime);

        Long comparisonSum = orderBlotterToCompare.stream().parallel().reduce(0L, (Long l, OrderBlotter orderBlotter) -> {
            l += orderBlotter.getCount();
            return l;
        }, Long::sum);

        Long todaysBlotterSum = todayOrderBlotter.stream().parallel().reduce(0L, (Long l, OrderBlotter orderBlotter) -> {
            l += orderBlotter.getCount();
            return l;
        }, Long::sum);

        long percentageChange = ((todaysBlotterSum - comparisonSum) * 100) / todaysBlotterSum;

        rebalanceScheduledFutureOrdersBasedOnCurrentTrend(simulateCurrentTime, percentageChange);
    }

    private List<OrderBlotter> getPreviousOrderBlottersForTimeInterval() {
        final Calendar comparisonEndTime = Calendar.getInstance();
        final Calendar comparisonStartTime = Calendar.getInstance();
        comparisonStartTime.add(Calendar.MINUTE, -intervalMinute);

        return simulatedOrderBlotterByInterval.stream().filter(orderBlotter -> {
            try {
                return (
                        DateUtil.stringToCalendar(orderBlotter.getCreationTime(), DateUtil.datetimeFormat).getTime()
                                .after(comparisonStartTime.getTime()) &&
                                DateUtil.stringToCalendar(orderBlotter.getCreationTime(), DateUtil.datetimeFormat).getTime()
                                        .before(comparisonEndTime.getTime())
                );
            } catch (ParseException ex) {
                log.error("Error in Parsing the OrderBlotter Creation Time: " + orderBlotter.getCreationTime());
            }

            return false;
        }).collect(Collectors.toList());
    }

    private List<OrderBlotter> getSimulatedOrderBlotterByInterval(Calendar simulateCurrentTime) {
        final Calendar tomorrowComparisonStartTime = (Calendar) simulateCurrentTime.clone();
        tomorrowComparisonStartTime.add(Calendar.MINUTE, -intervalMinute);
        return orderBlotterDao.findOrderBlottersByInterval(tomorrowComparisonStartTime, simulateCurrentTime);
    }

    private void rebalanceScheduledFutureOrdersBasedOnCurrentTrend(Calendar simulateCurrentTime, long percentageChange) {
        if (Math.abs(percentageChange) > 10) {
            log.trace("Percentage greater than 10%. Re-Balance all the future orders!");

            final ConcurrentHashMap<String, OrderScheduler.TaskFuturePair> taskFuturePairConcurrentHashMap = schedulerOrdersByGroupId.get(groupId);
            taskFuturePairConcurrentHashMap.forEach((groupKey, groupValue) -> {
                final OrderTask orderTask = groupValue.getOrderTask();
                final ScheduledFuture scheduledFuture = groupValue.getScheduledFuture();
                final Calendar timeToSend = orderTask.getTimeToSend();

                if (timeToSend.getTime().after(simulateCurrentTime.getTime())) {
                    log.trace("The order needs to be balanced: " + orderTask.getOrder());
                }
            });
        }
    }
}
