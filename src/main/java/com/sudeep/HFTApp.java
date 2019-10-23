package com.sudeep;

import com.sudeep.core.messageQueue.CreateTaskConsumer;
import com.sudeep.core.messageQueue.MQListener;
import com.sudeep.core.sender.strategy.DelayOneSender;
import com.sudeep.core.sender.strategy.Sender;
import com.sudeep.core.processor.Processor;
import com.sudeep.core.processor.TWAPProcessor;
import com.sudeep.core.processor.VWAPProcessor;
import com.sudeep.dao.OrderBlotterDao;
import com.sudeep.domain.Entity.Order;
import com.sudeep.domain.Entity.OrderBlotter;
import com.sudeep.domain.Entity.OrderBuilder;
import com.sudeep.domain.enums.OrderStatus;
import com.sudeep.domain.enums.OrderTransactionType;
import com.sudeep.domain.enums.OrderType;
import com.sudeep.exception.InvalidProcessorTypeException;
import com.sudeep.scheduler.OrderScheduler;
import com.sudeep.service.*;
import com.sudeep.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class HFTApp {
    private static final Logger log = LoggerFactory.getLogger(HFTApp.class);

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Input params are incorrect! Please pass processorType: TWAP/VWAP");
        }
        log.trace("Starting HFT App!");

        String processorType = args[0];
        final String traderId = "AA11233";
        final int brokerId = 1111122234;

        final String orderId = UUID.randomUUID().toString();
        final Order order = OrderBuilder
                .anOrder()
                .withOrderId(orderId)
                .withTraderID(traderId)
                .withTradingSymbol("AAPL")
                .withProduct("EQ")
                .withOrderTransactionType(OrderTransactionType.BUY)
                .withOrderType(OrderType.MarketOrder)
                .withTotalCount(1000000)
                .withOrderStatus(OrderStatus.WAITING)
                .withCreationTime(LocalDateTime.now().toString())
                .build();

        final Calendar tomorrowOpenTime = DateUtil.getTomorrowOpenTime();
        final Calendar tomorrowCloseTime = DateUtil.getTomorrowCloseTime();
        int intervalMinute = 1;
        OrderBlotterDao orderBlotterDao = new OrderBlotterDao();
        Processor processor;
        if (processorType.equalsIgnoreCase("TWAP")) {
            processor = new TWAPProcessor(tomorrowOpenTime, tomorrowCloseTime, intervalMinute);
        } else if (processorType.equalsIgnoreCase("VWAP") || processorType.equalsIgnoreCase("VWAPMinimizeSlippage")){
            intervalMinute = 10;
            processor = new VWAPProcessor(tomorrowOpenTime, tomorrowCloseTime, orderBlotterDao, intervalMinute);
        } else {
            throw new InvalidProcessorTypeException("Currently the app only support VWAP/TWAP Processor Types.");
        }

        Broker broker = new BrokerImpl(brokerId);
        BrokerService brokerService = new BrokerServiceImpl(broker);

        List<Order> orders = processor.process(order);
        Sender sender = new DelayOneSender(tomorrowOpenTime, tomorrowCloseTime, intervalMinute, broker);
        String groupId = sender.send(orders);
        if (log.isTraceEnabled()) {
            log.trace(String.format("groupId: %s", groupId));
        }

        MQListener mqListener = new MQListener();
        OrderScheduler orderScheduler = new OrderScheduler();
        CreateTaskConsumer createTaskConsumer = new CreateTaskConsumer(orderScheduler, brokerService);

        /*
         * Slippage can be based on variation of expectedPrice and executedPrice but currently basing this on variation of volume profile.
         */
        if (processorType.equalsIgnoreCase("VWAPMinimizeSlippage")) {
            final List<OrderBlotter> simulatedOrderBlotterByInterval = ((VWAPProcessor) processor).getSimulatedOrderBlotterByInterval();
            final ConcurrentHashMap<String, ConcurrentHashMap<String, OrderScheduler.TaskFuturePair>> schedulerOrdersByGroupId = orderScheduler.getGroups();
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new MinimizeSlippageManagerService(simulatedOrderBlotterByInterval, schedulerOrdersByGroupId, orderBlotterDao, groupId, intervalMinute), 3000 , 1000);
        }

        Thread.sleep(1000 * 5);
        new Thread(() -> {
            try {
                mqListener.listenCreate(createTaskConsumer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        //Print all orders on the Broker
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000 * 60);
                } catch (InterruptedException ex) {
                    log.error("InterruptedException: ", ex);
                }

                broker.printAllOrders();
            }
        }).start();
    }
}
