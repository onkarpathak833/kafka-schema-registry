package com.kafka.examples.domain;

public class Order {

    int id;
    Customer customer;
    Double orderTotal;
    String status;
    Address shippingAddress;
    Address billingAddress;
    boolean isGift;

    public Order(int id, Customer customer, Double orderTotal, String status, Address shippingAddress, boolean isGift) {
        this.id = id;
        this.customer = customer;
        this.orderTotal = orderTotal;
        this.status = status;
        this.shippingAddress = shippingAddress;
        this.billingAddress = shippingAddress;
        this.isGift = isGift;
    }


}
