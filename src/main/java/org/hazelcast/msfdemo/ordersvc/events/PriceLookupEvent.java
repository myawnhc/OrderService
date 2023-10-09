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
import java.math.BigDecimal;

public class PriceLookupEvent extends OrderEvent {

    public static final String QUAL_EVENT_NAME = "OrderService:PriceLookupEvent";
    public static final String ORDER_NUMBER = "doKey";
    public static final String EXTENDED_PRICE = "amount";
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String LOCATION = "location";
    public static final String QUANTITY = "quantity";

    private final String itemNumber;
    private final String location;
    private final int quantity;
    private final BigDecimal extendedPrice;

    public PriceLookupEvent(String orderNumber, String itemNumber, String location, int quantity, BigDecimal price) {
        setEventName(QUAL_EVENT_NAME);
        this.key = orderNumber;
        this.itemNumber = itemNumber;
        this.location = location;
        this.quantity = quantity;
        this.extendedPrice = price;
    }

    public PriceLookupEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.key = data.getString(ORDER_NUMBER);
        this.itemNumber = data.getString(ITEM_NUMBER);
        this.location = data.getString(LOCATION);
        this.quantity = data.getInt32(QUANTITY);
        this.extendedPrice = data.getDecimal(EXTENDED_PRICE);
    }

    public PriceLookupEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        this.key = row.getObject("key");
        this.itemNumber = row.getObject(ITEM_NUMBER);
        this.location = row.getObject(LOCATION);
        this.quantity = row.getObject(QUANTITY);
        this.extendedPrice = row.getObject(EXTENDED_PRICE);
        Long time = row.getObject(EVENT_TIME);
        if (time != null)
            setTimestamp(time);
    }

    @Override
    public Order apply(Order order) {
        order.setOrderNumber(getKey());
        order.setItemNumber(itemNumber);
        order.setLocation(location);
        order.setQuantity(quantity);
        order.setExtendedPrice(extendedPrice);
        //order.setWaitingOn(EnumSet.of(WaitingOn.CREDIT_CHECK, WaitingOn.RESERVE_INVENTORY));
        return order;
    }

    @Override
    public String toString() {
        return "PriceLookupEvent for order " + getKey() + " extended price " + extendedPrice;
    }

    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(getEventName())
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setString(ORDER_NUMBER, getOrderNumber())
                .setString(ITEM_NUMBER, itemNumber)
                .setString(LOCATION, location)
                .setInt32(QUANTITY, quantity)
                .setDecimal(EXTENDED_PRICE, extendedPrice)
                .build();
        return gr;
    }
}
