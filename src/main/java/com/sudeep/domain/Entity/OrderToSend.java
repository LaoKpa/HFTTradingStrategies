package com.sudeep.domain.Entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class OrderToSend {
    public final static String INIT = "INIT";
    public final static String SCHEDULED = "SCHEDULED";
    public final static String CREATED = "CREATED";
    public final static String CANCELLED = "CANCELLED";

    private String groupId;
    private String orderToSendId;
    private String brokerOrderId;
    private Order order;
    private String customerId;
    private int brokerId;
    private String datetime;
    private String status = INIT;
    private String cancelOrderId;
}
