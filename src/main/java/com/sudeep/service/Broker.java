package com.sudeep.service;

import com.sudeep.domain.Entity.Order;

import java.util.List;

public interface Broker {
    void sendOrder(Order order);

    List<Order> getAllOrders();

    void printAllOrders();

    int getBrokerId();
}
