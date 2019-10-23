package com.sudeep.core.sender.strategy;

import com.sudeep.domain.Entity.Order;
import com.sudeep.domain.enums.OrderType;
import com.sudeep.service.Broker;
import lombok.Getter;

import java.util.Calendar;
import java.util.List;

@Getter
public abstract class DelaySender implements Sender {
    private final Broker broker;
    private final Calendar startTime;
    private final Calendar endTime;
    private final int intervalMinute;

    public DelaySender(Calendar startTime, Calendar endTime, int intervalMinute, Broker broker) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.intervalMinute = intervalMinute;
        this.broker = broker;
    }

    public abstract String send(List<Order> orders);

    boolean checkOrder(Order order) {
        return !order.getOrderType().equals(OrderType.CancelOrder);
    }
}
