package com.sudeep.core.processor;

import com.sudeep.domain.Entity.Order;

import java.util.List;

public interface Processor {
    List<Order> process(Order order);
}
