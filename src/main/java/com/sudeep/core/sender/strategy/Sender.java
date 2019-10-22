package com.sudeep.core.sender.strategy;


import com.sudeep.domain.Entity.Order;

import java.util.List;

public interface Sender {

    Object send(List<Order> orders);
}
