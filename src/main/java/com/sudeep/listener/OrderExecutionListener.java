package com.sudeep.listener;

import com.sudeep.domain.Entity.Order;

public interface OrderExecutionListener {

    void onOrderExecution(Order order);
}
