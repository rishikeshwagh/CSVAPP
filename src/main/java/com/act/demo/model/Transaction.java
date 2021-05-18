package com.act.demo.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Transaction {

    private long transId;
    private String transProductType;
    private String transType;
    private Long transAmount;
    private Long transParentId;
    private boolean priority;
    private String empId;
}
