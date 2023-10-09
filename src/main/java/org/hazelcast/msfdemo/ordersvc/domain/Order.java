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
package org.hazelcast.msfdemo.ordersvc.domain;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import org.hazelcast.eventsourcing.event.DomainObject;

import java.math.BigDecimal;

public class Order implements DomainObject<String> {

    public static final String QUAL_DO_NAME = "OrderService:Order";

    public static final String ORDER_NUMBER = "key";
    public static final String ACCOUNT_NUMBER = "acctNumber";
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String LOCATION = "location";
    public static final String EXTENDED_PRICE = "price";
    public static final String QUANTITY = "quantity";

    private String orderNumber;
    private String acctNumber;
    private String itemNumber;
    private String location;
    private BigDecimal extendedPrice;
    private int quantity;
    //private EnumSet<WaitingOn> waitingOn;

    public Order() {}

    public Order(GenericRecord fromGR) {
        this.orderNumber = fromGR.getString(ORDER_NUMBER);
        this.itemNumber = fromGR.getString(ITEM_NUMBER);
        this.acctNumber = fromGR.getString(ACCOUNT_NUMBER);
        this.location = fromGR.getString(LOCATION);
        this.extendedPrice = fromGR.getDecimal(EXTENDED_PRICE);
        this.quantity = fromGR.getInt32(QUANTITY);
        if (this.itemNumber == null) {
            System.out.println("Null itemNumber");
        }
    }

    public String getKey() { return orderNumber; }

    // Commands
    public void setOrderNumber(String orderNum) { this.orderNumber = orderNum; }
    public String getOrderNumber() { return this.orderNumber; }

    public void setAcctNumber(String acctNum) { this.acctNumber = acctNum; }
    public String getAcctNumber() { return this.acctNumber; }

    public void setItemNumber(String itemNum) { this.itemNumber = itemNum; }
    public String getItemNumber() { return this.itemNumber; }

    public void setLocation(String location) { this.location = location; }
    public String getLocation() { return location; }

    public void setQuantity(int quantity) { this.quantity = quantity; }
    public int getQuantity() { return quantity; }

    public void setExtendedPrice(BigDecimal price) { this.extendedPrice = price; }
    public BigDecimal getExtendedPrice() { return extendedPrice; }

//    public EnumSet<WaitingOn> getWaitingOn() {
//        return waitingOn;
//    }
//    public void setWaitingOn(EnumSet<WaitingOn> waitingOn) {
//        this.waitingOn = waitingOn;
//    }

    @Override
    public String toString() {
        return "Order " + orderNumber + " I:" + itemNumber + " L:" + location + " A:" + acctNumber + " Q:" + quantity;
    }

    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(QUAL_DO_NAME)
                .setString(ORDER_NUMBER, orderNumber)
                .setString(ACCOUNT_NUMBER, acctNumber)
                .setString(ITEM_NUMBER, itemNumber)
                .setString(LOCATION, location)
                .setDecimal(EXTENDED_PRICE, extendedPrice)
                .setInt32(QUANTITY, quantity)
                .build();
        return gr;
    }
}
