package com.sudeep.domain.Entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class OrderBlotter {
    private String orderBlotterId;
    private int count;
    private int price;
    private String creationTime;
}
