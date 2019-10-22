package com.sudeep.service;

import com.sudeep.domain.Entity.Order;

public interface BrokerService {
    void sendOrder(Order order);
}
