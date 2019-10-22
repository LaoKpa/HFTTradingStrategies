package com.sudeep.core.MessageQueue;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.sudeep.domain.Entity.OrderToSend;
import com.sudeep.domain.enums.TaskConsumerCommand;
import com.sudeep.exception.InvalidTaskConsumerCommand;
import com.sudeep.scheduler.OrderScheduler;
import com.sudeep.task.OrderTaskFactory;
import com.sudeep.util.DateUtil;
import com.sudeep.service.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Calendar;

public class CreateTaskConsumer implements TaskConsumer {
    private final static Logger logger = LoggerFactory.getLogger("TaskConsumer");

    private final OrderScheduler scheduler;
    private final OrderTaskFactory orderTaskFactory;

    public CreateTaskConsumer(BrokerService brokerService) {
        scheduler = new OrderScheduler();
        orderTaskFactory = new OrderTaskFactory(brokerService);
    }

    public DeliverCallback consume(Channel channel) {
        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            logger.info(String.format("[CreateTaskConsumer.consume] Raw Message: %s", message));

            JSONObject jsonMessage = JSON.parseObject(message);
            String type = jsonMessage.getString("type");
            if (type == null)
                throw new InvalidTaskConsumerCommand("Invalid Command in Create: null");

            if (TaskConsumerCommand.CREATE.equals(type)) {
                OrderToSend orderToSend = jsonMessage.getObject("body", OrderToSend.class);
                orderToSend.setStatus(OrderToSend.SCHEDULED);
                logger.trace(String.format("[CreateTaskConsumer.consume.create. %s] %s", orderToSend.getOrderToSendId(), JSON.toJSONString(orderToSend)));

                if (orderToSend.getOrder().getTotalCount() == 0) {
                    logger.trace(String.format("[CreateTaskConsumer.consume.create. %s] TotalCount = 0", orderToSend.getOrderToSendId()));
                    return;
                }
                try {
                    Calendar calendar = DateUtil.stringToCalendar(orderToSend.getDatetime(), DateUtil.datetimeFormat);
                    scheduler.schedule(orderTaskFactory.create(orderToSend, channel, delivery), calendar);
                    logger.trace(String.format("[CreateTaskConsumer.consume.create. %s %s] Success", orderToSend.getOrderToSendId(), orderToSend));
                } catch (ParseException ex) {
                    logger.error(String.format("[CreateTaskConsumer.consume.create.%s] Error", orderToSend.getOrderToSendId()), ex);
                }
            } else {
                throw new InvalidTaskConsumerCommand(String.format("Invalid Command in Create: %s", type));
            }
        };
    }
}
