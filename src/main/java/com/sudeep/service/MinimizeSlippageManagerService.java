package com.sudeep.service;

import com.sudeep.domain.Entity.OrderBlotter;
import com.sudeep.scheduler.OrderScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class MinimizeSlippageManagerService extends TimerTask {
    private static final Logger log = LoggerFactory.getLogger(MinimizeSlippageManagerService.class);

    private final List<OrderBlotter> simulatedOrderBlotterByInterval;
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, OrderScheduler.TaskFuturePair>> schedulerOrdersByGroupId;

    public MinimizeSlippageManagerService(List<OrderBlotter> simulatedOrderBlotterByInterval, ConcurrentHashMap<String, ConcurrentHashMap<String, OrderScheduler.TaskFuturePair>> schedulerOrdersByGroupId) {
        this.simulatedOrderBlotterByInterval = simulatedOrderBlotterByInterval;
        this.schedulerOrdersByGroupId = schedulerOrdersByGroupId;
    }

    @Override
    public void run() {
        updateScheduledOrdersBasedOnVolumeProfile();
    }

    private void updateScheduledOrdersBasedOnVolumeProfile() {
        if (log.isTraceEnabled()) {
            log.trace("Re-Balance the scheduled orders based on the change of volume profile compared to the earlier volume profile!");
        }
    }
}
