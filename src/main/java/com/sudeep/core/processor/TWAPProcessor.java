package com.sudeep.core.processor;

import com.sudeep.domain.Entity.Order;
import com.sudeep.util.DateUtil;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

@Getter
public class TWAPProcessor implements Processor {
    private final static Logger logger = LoggerFactory.getLogger("TWAPProcessor");

    private final int interval;
    private final Calendar startTime;
    private final Calendar endTime;

    public TWAPProcessor(Calendar startTime, Calendar endTime, int interval) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.interval = interval;
    }

    public List<Order> process(Order order) {
        logger.info("[TWAPProcessor] startTime: " + DateUtil.datetimeFormat.format(startTime.getTime()));
        logger.info("[TWAPProcessor] endTime: " + DateUtil.datetimeFormat.format(endTime.getTime()));
        logger.info("[TWAPProcessor.process] " + order.toString());

        int total = order.getTotalCount();
        int slice = DateUtil.getMinuteInterval(startTime, endTime) / interval;
        int mean = total / slice;
        logger.info("[TWAPProcessor.process] Slice: " + slice + " Mean: " + mean);

        List<Order> orders = new ArrayList<>();

        int gap = total - mean * slice;

        for (int i = 0; i < slice; i++) {
            Order o = Order.copyOrder(order);
            o.setTotalCount(mean);
            orders.add(o);
        }

        Order temp = orders.get(0);
        temp.setTotalCount(temp.getTotalCount() + gap);
        return orders;
    }
}
