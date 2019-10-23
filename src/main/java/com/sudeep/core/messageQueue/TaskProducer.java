package com.sudeep.core.messageQueue;

import com.alibaba.fastjson.JSONObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.sudeep.domain.Entity.OrderToSend;
import com.sudeep.domain.enums.TaskConsumerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class TaskProducer {
    private final static Logger logger = LoggerFactory.getLogger("TaskProducer");

    private final static String QUEUE_NAME = "FutureTask";
    private final static String MQ_HOST = "localhost";
    private final static String EXCHANGE = "Cancel";
    private final static ConnectionFactory factory = getFactory();
    private final static Connection connection = getConnection();

    public static void create(OrderToSend ots){
        logger.info("[TaskProducer.create] OrderToSend: " + ots);
        JSONObject message = new JSONObject();
        message.put("body", ots);
        message.put("type", TaskConsumerCommand.CREATE);
        produce(message.toJSONString());
    }
    private static boolean produce(String message){
        logger.info(" [TaskProducer.produce] To send: " + message);
        try {
            Channel channel = Objects.requireNonNull(connection).createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            logger.info(" [TaskProducer.produce] Sent: " + message);
            return true;
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    private static boolean broadcast(String message){
        logger.info(" [TaskProducer.broadcast] To send: " + message);
        try {
            Channel channel = Objects.requireNonNull(connection).createChannel();
            channel.exchangeDeclare(EXCHANGE, "fanout");

            channel.basicPublish(EXCHANGE, "", null, message.getBytes());
            logger.info(" [TaskProducer.broadcast] Sent: " + message);
            return true;
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static boolean cancel(String groupId){
        logger.info("[TaskProducer.cancel] GroupId: " + groupId);
        JSONObject message = new JSONObject();
        message.put("body", groupId);
        message.put("type", TaskConsumerCommand.CANCEL);
        return broadcast(message.toJSONString());
    }

    private static ConnectionFactory getFactory(){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(MQ_HOST);
        return factory;
    }

    private static Connection getConnection(){
        try {
            return TaskProducer.factory.newConnection();
        }
        catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }
}
