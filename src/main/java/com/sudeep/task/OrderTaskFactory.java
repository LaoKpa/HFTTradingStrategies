package com.sudeep.task;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.sudeep.domain.Entity.Order;
import com.sudeep.domain.Entity.OrderToSend;
import com.sudeep.service.BrokerService;

public class OrderTaskFactory {
    private final BrokerService brokerService;

    public OrderTaskFactory(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    public OrderTask create(OrderToSend orderToSend, Channel channel, Delivery delivery){
        OrderTask orderTask = new OrderTask(brokerService);

        orderTask.setOrderToSend(orderToSend);

        Order orderWithOtsId = orderToSend.getOrder();
        orderTask.setOrder(orderWithOtsId);
        orderTask.setTraderSideUsername(orderToSend.getCustomerId());
        orderTask.setBrokerId(orderToSend.getBrokerId());
        orderTask.setId(orderToSend.getOrderToSendId());
        orderTask.setChannel(channel);
        orderTask.setDelivery(delivery);
        return orderTask;
    }
}
