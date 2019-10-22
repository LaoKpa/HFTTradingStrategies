package com.sudeep.domain.Entity;

import com.sudeep.domain.enums.OrderStatus;
import com.sudeep.domain.enums.OrderTransactionType;
import com.sudeep.domain.enums.OrderType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class Order {
    private String orderId;
    private String customerId;
    private String tradingSymbol;
    private String product;
    private int price;
    private OrderTransactionType orderTransactionType;
    private OrderType orderType;
    private int count;
    private int totalCount;
    private String futureName;
    private OrderStatus orderStatus;
    private String creationTime;

    public Order() {
    }

    public static Order copyOrder(Order order) {
        return OrderBuilder
                .anOrder()
                .withOrderId(order.getOrderId())
                .withTraderID(order.getCustomerId())
                .withTradingSymbol(order.getTradingSymbol())
                .withProduct(order.getProduct())
                .withOrderTransactionType(order.getOrderTransactionType())
                .withPrice(order.getPrice())
                .withCount(order.getCount())
                .withTotalCount(order.getTotalCount())
                .withFutureName(order.getFutureName())
                .withOrderType(order.getOrderType())
                .withOrderStatus(order.getOrderStatus())
                .build();
    }
}
