/*
 * Copyright 2018-2022 Hazelcast, Inc
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.msfdemo.ordersvc.domain.Order;

import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.InventoryReserved;

public class ReserveInventoryEvent extends OrderEvent {

    public static final String QUAL_EVENT_NAME = "OrderService:ReserveInventoryEvent";
    public static final String ORDER_NUMBER = "doKey";
    public static final String ACCT_NUMBER = "accountNumber";
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String QUANTITY = "quantity";
    public static final String LOCATION = "location";
    public static final String FAILURE_REASON = "description";

    private String accountNumber;
    private String itemNumber;
    private String location;
    private int quantity;
    private String failureReason;

    public ReserveInventoryEvent(String orderNumber, String acctNumber, String itemNumber, String location, int quantity) {
        setEventName(QUAL_EVENT_NAME);
        this.key = orderNumber;
        this.accountNumber = acctNumber;
        this.itemNumber = itemNumber;
        this.location = location;
        this.quantity = quantity;
    }

    public ReserveInventoryEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.key = data.getString(ORDER_NUMBER);
        this.accountNumber = data.getString(ACCT_NUMBER);
        this.itemNumber = data.getString(ITEM_NUMBER);
        this.location = data.getString(LOCATION);
        this.quantity = data.getInt32(QUANTITY);
        this.failureReason = data.getString(FAILURE_REASON);
    }

    public void setFailureReason(String reason) {
        this.failureReason = reason;
    }

    public ReserveInventoryEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        this.key = row.getObject("key");
        this.accountNumber = row.getObject(ACCT_NUMBER);
        this.itemNumber = row.getObject(ITEM_NUMBER);
        this.location = row.getObject(LOCATION);
        this.quantity = row.getObject(QUANTITY);
        this.failureReason = row.getObject(FAILURE_REASON);
        Long time = row.getObject(EVENT_TIME);
        if (time != null)
            setTimestamp(time);
    }

    public String getAccountNumber() { return accountNumber; }
    public String getItemNumber() { return itemNumber; }
    public String getLocation() { return location; }
    public int getQuantity() { return quantity; }

    public Order apply(Order order) {
        // May in some future implementation alter quantity, for partial ship,
        // but we're not doing that currently.
        order.setQuantity(quantity);
//        EnumSet<WaitingOn> waits = order.getWaitingOn();
//        waits.remove(WaitingOn.RESERVE_INVENTORY);
//        if (waits.isEmpty()) {
//            waits.add(WaitingOn.CHARGE_ACCOUNT);
//            waits.add(WaitingOn.PULL_INVENTORY);
//        }
        return order;
    }

    @Override
    public String toString() {
        return "ReserveInventoryEvent I:" + itemNumber + " Q:" + quantity;
    }

    @Override
    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(getEventName())
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setString(ORDER_NUMBER, getOrderNumber())
                .setString(ACCT_NUMBER, accountNumber)
                .setString(ITEM_NUMBER, itemNumber)
                .setString(LOCATION, location)
                .setInt32(QUANTITY, quantity)
                .setString(FAILURE_REASON, failureReason)
                .build();
        return gr;
    }
}


