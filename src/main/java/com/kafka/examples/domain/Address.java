package com.kafka.examples.domain;

public class Address {
    String line1;
    String line2;
    String state;
    String district;
    String country;
    long pinCode;

    public Address(String line1, String line2, String state, String district, String country, long pinCode) {
        this.line1 = line1;
        this.line2 = line2;
        this.state = state;
        this.district = district;
        this.country = country;
        this.pinCode = pinCode;
    }
}
