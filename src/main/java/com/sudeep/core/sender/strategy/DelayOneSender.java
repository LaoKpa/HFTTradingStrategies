package com.sudeep.core.sender.strategy;

import com.sudeep.core.messageQueue.TaskProducer;
import com.sudeep.domain.Entity.Order;
import com.sudeep.domain.Entity.OrderToSend;
import com.sudeep.service.Broker;
import com.sudeep.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.List;
import java.util.UUID;

public class DelayOneSender extends DelaySender {
    private final static Logger logger = LoggerFactory.getLogger("DelayOneSender");

    public DelayOneSender(Calendar startTime, Calendar endTime, int intervalMinute, Broker broker) {
        super(startTime, endTime, intervalMinute, broker);
    }

    public String send(List<Order> orders) {
        if (logger.isTraceEnabled()) {
            logger.trace(String.format("DelayOneSender sending Orders: %s", orders));
        }

        orders.forEach(order -> {
            if (!checkOrder(order))
                throw new RuntimeException("CancelOrder is not allowed in DelaySender.");
        });

        String groupId = UUID.randomUUID().toString();
        int interval = getIntervalMinute();
        Calendar cur = (Calendar) getStartTime().clone();

        orders.forEach(order -> {
            if (order.getTotalCount() == 0)
                return;

            OrderToSend orderToSend = new OrderToSend();
            orderToSend.setStatus(OrderToSend.INIT);

            orderToSend.setGroupId(groupId);
            orderToSend.setBrokerId(getBroker().getBrokerId());
            String orderToSendId = UUID.randomUUID().toString();
            orderToSend.setOrderToSendId(orderToSendId);
            orderToSend.setDatetime(DateUtil.calendarToString(cur, DateUtil.datetimeFormat));
            orderToSend.setOrder(order);
            orderToSend.setCustomerId(order.getCustomerId());

            TaskProducer.create(orderToSend);
            cur.add(Calendar.MINUTE, interval);
        });

        return groupId;
    }
}
