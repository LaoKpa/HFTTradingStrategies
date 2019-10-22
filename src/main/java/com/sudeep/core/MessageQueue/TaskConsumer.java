package com.sudeep.core.MessageQueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public interface TaskConsumer {
    DeliverCallback consume(Channel channel);
}
