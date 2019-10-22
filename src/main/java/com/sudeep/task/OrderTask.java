package com.sudeep.task;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.sudeep.domain.Entity.Order;
import com.sudeep.domain.Entity.OrderToSend;
import com.sudeep.util.DateUtil;
import com.sudeep.service.BrokerService;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;

@Getter
@Setter
public class OrderTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger("OrderTask");

    private OrderToSend orderToSend;
    private String id;
    private Calendar timeToSend;
    private String traderSideUsername;
    private Integer brokerId;
    private Order order;
    private Channel channel;
    private Delivery delivery;
    private BrokerService brokerService;

    public OrderTask(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    @Override
    public void run() {
        logger.info("[OrderTask.execute." + getId() + "] Time: " + DateUtil.calendarToString(Calendar.getInstance(), DateUtil.datetimeFormat));

        //ToDo - To implement - Currently sending orders to a Dummy Broker
        logger.info("Send the order to Broker. OrderToSend: " + orderToSend + " timeToSend: " + timeToSend.getTime().toString() + " order: " + order);
        brokerService.sendOrder(order);
        sendACK();
    }

    private void sendACK() {
        try {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            logger.info("[OrderTask.execute." + getId() + "] ACK is sent");
        } catch (IOException e) {
            logger.info("[OrderTask.execute." + getId() + "] Error: RabbitMQ ACK lost");
            e.printStackTrace();
        }
    }
}
