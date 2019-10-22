package com.sudeep.dao;

import com.sudeep.domain.Entity.OrderBlotter;
import com.sudeep.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderBlotterDao {
    private static final Logger logger = LoggerFactory.getLogger("OrderBlotterDao");

    //ToDO
    public List<OrderBlotter> findOrderBlottersByInterval(Calendar startTime, Calendar endTime){
        return simulateOrderBlotter(startTime, endTime);
    }

    private List<OrderBlotter> simulateOrderBlotter(Calendar startTime, Calendar endTime) {
        List<OrderBlotter> orderBlotterList = new ArrayList<>();
        Calendar startSearchCalendar = (Calendar) startTime.clone();
        Calendar endSearchCalendar = (Calendar) endTime.clone();

        int defaultMinuteDuration = 1;
        while (startSearchCalendar.getTime().before(endSearchCalendar.getTime())) {
            String orderBlotterId = UUID.randomUUID().toString();
            OrderBlotter orderBlotter = new OrderBlotter();
            orderBlotter.setOrderBlotterId(orderBlotterId);
            orderBlotter.setCount((int) (10000 * Math.random()));
            orderBlotter.setCreationTime(DateUtil.datetimeFormat.format(startSearchCalendar.getTime()));
            orderBlotterList.add(orderBlotter);
            startSearchCalendar.add(Calendar.MINUTE, defaultMinuteDuration);
        }

        logger.trace("Simulated orderBlotterList: " + orderBlotterList);
        AtomicInteger sum = new AtomicInteger();
        orderBlotterList.forEach(orderBlotter -> {
            final int count = orderBlotter.getCount();
            sum.addAndGet(count);
        });

        logger.trace("Total number of orders: " + sum);
        return orderBlotterList;
    }
}
