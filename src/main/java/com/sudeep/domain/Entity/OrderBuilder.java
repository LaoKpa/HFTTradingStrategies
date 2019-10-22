package com.sudeep.domain.Entity;

import com.sudeep.domain.enums.OrderStatus;
import com.sudeep.domain.enums.OrderTransactionType;
import com.sudeep.domain.enums.OrderType;

public final class OrderBuilder {
    private String orderId;
    private String traderID;
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

    private OrderBuilder() {
    }

    public static OrderBuilder anOrder() {
        return new OrderBuilder();
    }

    public OrderBuilder withOrderId(String orderId) {
        this.orderId = orderId;
        return this;
    }

    public OrderBuilder withTraderID(String traderID) {
        this.traderID = traderID;
        return this;
    }

    public OrderBuilder withTradingSymbol(String tradingSymbol) {
        this.tradingSymbol = tradingSymbol;
        return this;
    }

    public OrderBuilder withProduct(String product) {
        this.product = product;
        return this;
    }

    public OrderBuilder withPrice(int price) {
        this.price = price;
        return this;
    }

    public OrderBuilder withOrderTransactionType(OrderTransactionType orderTransactionType) {
        this.orderTransactionType = orderTransactionType;
        return this;
    }

    public OrderBuilder withOrderType(OrderType orderType) {
        this.orderType = orderType;
        return this;
    }

    public OrderBuilder withCount(int count) {
        this.count = count;
        return this;
    }

    public OrderBuilder withTotalCount(int totalCount) {
        this.totalCount = totalCount;
        return this;
    }

    public OrderBuilder withFutureName(String futureName) {
        this.futureName = futureName;
        return this;
    }

    public OrderBuilder withOrderStatus(OrderStatus orderStatus) {
        this.orderStatus = orderStatus;
        return this;
    }

    public OrderBuilder withCreationTime(String creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    public Order build() {
        Order order = new Order();
        order.setOrderId(orderId);
        order.setCustomerId(traderID);
        order.setTradingSymbol(tradingSymbol);
        order.setProduct(product);
        order.setPrice(price);
        order.setOrderTransactionType(orderTransactionType);
        order.setOrderType(orderType);
        order.setCount(count);
        order.setTotalCount(totalCount);
        order.setFutureName(futureName);
        order.setOrderStatus(orderStatus);
        order.setCreationTime(creationTime);
        return order;
    }
}
