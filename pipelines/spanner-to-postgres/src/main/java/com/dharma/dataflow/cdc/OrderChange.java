package com.dharma.dataflow.cdc;

import java.io.Serializable;

/**
 * One row's worth of change, already flattened out of Spanner's DataChangeRecord
 * into plain fields. This is what actually flows through the pipeline and gets
 * handed to the Postgres writer.
 *
 * Needs to be Serializable since Beam ships elements between pipeline stages,
 * and for DirectRunner that can even mean serializing across threads.
 */
public class OrderChange implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String orderId;
    private final String customerName; // null for DELETE
    private final String orderDate;    // null for DELETE, ISO format e.g. 2026-08-11
    private final String amount;       // null for DELETE, kept as String to avoid float precision issues
    private final String status;       // null for DELETE
    private final String modType;      // INSERT, UPDATE, or DELETE
    private final String spannerCommitTimestamp;

    public OrderChange(String orderId, String customerName, String orderDate,
                        String amount, String status, String modType,
                        String spannerCommitTimestamp) {
        this.orderId = orderId;
        this.customerName = customerName;
        this.orderDate = orderDate;
        this.amount = amount;
        this.status = status;
        this.modType = modType;
        this.spannerCommitTimestamp = spannerCommitTimestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public String getAmount() {
        return amount;
    }

    public String getStatus() {
        return status;
    }

    public String getModType() {
        return modType;
    }

    public String getSpannerCommitTimestamp() {
        return spannerCommitTimestamp;
    }

    @Override
    public String toString() {
        return "OrderChange{"
                + "orderId='" + orderId + '\''
                + ", modType='" + modType + '\''
                + ", customerName='" + customerName + '\''
                + ", orderDate='" + orderDate + '\''
                + ", amount='" + amount + '\''
                + ", status='" + status + '\''
                + ", spannerCommitTimestamp='" + spannerCommitTimestamp + '\''
                + '}';
    }
}
