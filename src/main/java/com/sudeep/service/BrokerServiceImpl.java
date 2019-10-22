package com.sudeep.service;

import com.sudeep.domain.Entity.Order;

public class BrokerServiceImpl implements BrokerService {
    private final Broker broker;

    public BrokerServiceImpl(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void sendOrder(Order order) {
        broker.sendOrder(order);
    }
}
