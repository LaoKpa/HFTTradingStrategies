package com.sudeep.core.processor;

import com.alibaba.fastjson.JSON;
import com.sudeep.dao.OrderBlotterDao;
import com.sudeep.domain.Entity.Order;
import com.sudeep.domain.Entity.OrderBlotter;
import com.sudeep.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;

public class VWAPProcessor implements Processor {
    private static final Logger logger = LoggerFactory.getLogger(VWAPProcessor.class);

    private final Calendar startTime;
    private final Calendar endTime;
    private final OrderBlotterDao orderBlotterDao;
    private final Integer intervalMinute;

    //Since currently the OrderBlotter is simulated, changing it to make it retrievable.
    private List<OrderBlotter> simulatedOrderBlotterByInterval;

    public VWAPProcessor(Calendar startTime, Calendar endTime, OrderBlotterDao od, Integer intervalMinute) {
        this.orderBlotterDao = od;
        this.intervalMinute = intervalMinute;

        Calendar startSearchCalendar = (Calendar) startTime.clone();
        startSearchCalendar.add(Calendar.DAY_OF_MONTH, -1);
        this.startTime = startSearchCalendar;

        Calendar endSearchCalendar = (Calendar) endTime.clone();
        endSearchCalendar.add(Calendar.DAY_OF_MONTH, -1);
        this.endTime = endSearchCalendar;
    }

    public List<Order> process(Order order) {
        logger.info("[VWAPProcessor.process] StartTime: " + DateUtil.calendarToString(startTime, DateUtil.datetimeFormat));
        logger.info("[VWAPProcessor.process] EndTime: " + DateUtil.calendarToString(endTime, DateUtil.datetimeFormat));
        logger.info("[VWAPProcessor.process] Order: " + JSON.toJSONString(order));

        simulatedOrderBlotterByInterval = orderBlotterDao.findOrderBlottersByInterval(startTime, endTime);
        double[] percents = getPercentage(simulatedOrderBlotterByInterval);

        List<Order> splitOrder = new ArrayList<>(percents.length);

        int sum = order.getTotalCount();
        int splitSum = 0;

        for (double percent : percents) {
            int splitCnt = (int) Math.floor(percent * sum);
            splitSum += splitCnt;
            Order split = makeSplitOrder(order, splitCnt);
            splitOrder.add(split);
        }
        if (splitSum < sum) {
            Order first = splitOrder.get(0);
            first.setTotalCount(first.getTotalCount() + (sum - splitSum));
        }
        logger.info("[VWAPProcessor.process] Result: " + JSON.toJSONString(splitOrder));
        return splitOrder;
    }

    private Order makeSplitOrder(Order origin, int count) {
        Order split = Order.copyOrder(origin);
        split.setTotalCount(count);
        return split;
    }

    private int getIndex(Calendar target) {
        int minutes = DateUtil.getMinuteInterval(startTime, target);
        return (int) Math.floor((double) minutes / intervalMinute);
    }

    private double[] getPercentage(List<OrderBlotter> orderBlotters) {
        int slice = getIndex(endTime);

        double[] res = new double[slice];

        Long sum = orderBlotters.stream().parallel().reduce(0L, (Long l, OrderBlotter orderBlotter) -> {
            l += orderBlotter.getCount();
            return l;
        }, Long::sum);

        logger.trace("Sum: " + sum);
        orderBlotters.sort(Comparator.comparing(OrderBlotter::getCreationTime));

        orderBlotters.forEach(ob -> {
            try {
                logger.trace("[VWAPProcessor.getPercentage] Created Time: " + ob.getCreationTime());
                Calendar createdTime = DateUtil.stringToCalendar(ob.getCreationTime(), DateUtil.datetimeFormat);
                int index = getIndex(createdTime);
                logger.trace("[VWAPProcessor.getPercentage] Index: " + index);
                res[index] += ob.getCount();
            } catch (ParseException ex) {
                logger.error("Exception encountered while parsing the creationTime: " + ex);
            }
        });

        for (int i = 0; i < slice; ++i) {
            res[i] = res[i] / sum;
        }

        double resSum = 0;
        for (int i = 0; i < slice; ++i) {
            resSum = resSum + res[i];
        }
        logger.trace("resSum: " + resSum);

        return res;
    }

    public List<OrderBlotter> getSimulatedOrderBlotterByInterval() {
        return simulatedOrderBlotterByInterval;
    }
}
