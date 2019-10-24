package com.sudeep.service;

import com.sudeep.dao.OrderBlotterDao;
import com.sudeep.domain.Entity.Order;
import com.sudeep.domain.Entity.OrderBlotter;
import com.sudeep.scheduler.OrderScheduler;
import com.sudeep.task.OrderTask;
import com.sudeep.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
        if (schedulerOrdersByGroupId.get(groupId) != null) {
            updateScheduledOrdersBasedOnCurrentTrend();
        }
    }

    private void updateScheduledOrdersBasedOnCurrentTrend() {
        if (log.isTraceEnabled()) {
            log.trace("Update the scheduled orders based on the change of volume profile compared to the previous volume profile which was used to split the orders!");
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

        updateScheduledFutureOrdersBasedOnCurrentTrend(simulateCurrentTime, percentageChange);
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

    private void updateScheduledFutureOrdersBasedOnCurrentTrend(Calendar simulateCurrentTime, long percentageChange) {
        if (Math.abs(percentageChange) > 20) {
            log.trace("Percentage greater than 20%. Update all the future orders to reflect the change in today's trend!");

            AtomicInteger sumOrderCount = new AtomicInteger();
            final ConcurrentHashMap<String, OrderScheduler.TaskFuturePair> scheduledFutureOrdersForGroupId = schedulerOrdersByGroupId.get(groupId);

            List<Order> orderList = new ArrayList<>();
            scheduledFutureOrdersForGroupId
                    .values()
                    .stream()
                    .map(OrderScheduler.TaskFuturePair::getOrderTask)
                    .map(OrderTask::getOrder)
                    .forEach(order -> {
                        final int totalCount = order.getTotalCount();
                        sumOrderCount.addAndGet(totalCount);
                        orderList.add(order);
                    });

            List<Double> currentPercentageSplit = new ArrayList<>();
            orderList.forEach(order -> {
                final int totalCount = order.getTotalCount();
                final int sumOrderCountVal = sumOrderCount.get();
                double percentageSplit = (totalCount * 1.0) / sumOrderCountVal;
                currentPercentageSplit.add(percentageSplit);
            });

            final List<Double> doubles = updatePercentageSplit(currentPercentageSplit, simulateCurrentTime, percentageChange);

            scheduledFutureOrdersForGroupId
                    .values()
                    .stream()
                    .map(OrderScheduler.TaskFuturePair::getOrderTask)
                    .forEach(orderTask -> {
                        final Calendar timeToSend = orderTask.getTimeToSend();
                        final Order order = orderTask.getOrder();

                        if (timeToSend.getTime().after(simulateCurrentTime.getTime())) {
                            log.trace("The order needs to be updated: " + orderTask.getOrder());
                            log.trace("Update the order count based on new percentage split.");
                            //order.setTotalCount();
                        }
                    });
        }
    }

    private List<Double> updatePercentageSplit(List<Double> currentPercentageSplit, Calendar simulateCurrentTime, long percentageChange) {
        //ToDo - Pending implement change in currentPercentageSplit.
        return currentPercentageSplit;
    }
}
