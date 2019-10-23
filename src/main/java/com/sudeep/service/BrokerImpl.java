package com.sudeep.service;

import com.sudeep.domain.Entity.Order;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Getter
public class BrokerImpl implements Broker {
    private static final Logger log = LoggerFactory.getLogger(Broker.class);

    private final int brokerId;
    private final List<Order> orderList = new ArrayList<>();

    public BrokerImpl(int brokerId) {
        this.brokerId = brokerId;
    }

    @Override
    public void sendOrder(Order order) {
        orderList.add(order);
    }

    @Override
    public List<Order> getAllOrders() {
        return orderList;
    }

    @Override
    public void printAllOrders() {
        log.trace("******* Start Orders placed on the Broker so far ********** ");
        orderList.forEach(order -> log.trace("Order: " + order));
        log.trace("******* End Orders placed on the Broker so far ********** ");
    }
}
