package com.sudeep.core.messageQueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class MQListener {
    private final static Logger logger = LoggerFactory.getLogger("MQListener");

    private final static String QUEUE_NAME = "FutureTask";
    private final static String MQ_HOST = "localhost";
    private final static boolean AUTO_ACK = false;
    private final static String EXCHANGE = "Cancel";

    private final static ConnectionFactory factory = getFactory();
    private final static Connection connection = getConnection();

    public void listenCreate(TaskConsumer taskConsumer) throws Exception{
        Channel channel = Objects.requireNonNull(connection).createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.basicConsume(QUEUE_NAME, AUTO_ACK, taskConsumer.consume(channel), consumerTag -> {});
    }

    public void listenCancel(TaskConsumer taskConsumer)throws Exception{
        Channel channel = Objects.requireNonNull(connection).createChannel();

        channel.exchangeDeclare(EXCHANGE, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE, "");
        channel.basicConsume(queueName, AUTO_ACK, taskConsumer.consume(channel), consumerTag -> {});
    }

    private static ConnectionFactory getFactory(){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(MQ_HOST);
        return factory;
    }

    private static Connection getConnection(){
        try {
            return MQListener.factory.newConnection();
        }
        catch(Exception ex){
            logger.error("Error while trying to get the new connection: ", ex);
            return null;
        }
    }
}
