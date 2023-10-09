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

public class CollectPaymentEvent extends OrderEvent {

    public static final String QUAL_EVENT_NAME = "OrderService:CollectPaymentEvent";
    public static final String ACCOUNT_NUMBER = "accountNumber";
    public static final String AMOUNT_CHARGED = "amount";
    public static final String ORDER_NUMBER = "doKey";

    private String accountNumber;
    private BigDecimal amountCharged;

    public CollectPaymentEvent(String orderNumber, String accountNumber, BigDecimal amountCharged) {
        setEventName(QUAL_EVENT_NAME);
        this.key = orderNumber;
        this.accountNumber = accountNumber;
        this.amountCharged = amountCharged;
    }

    public CollectPaymentEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.key = data.getString(ORDER_NUMBER);
        this.accountNumber = data.getString(ACCOUNT_NUMBER);
        this.amountCharged = data.getDecimal(AMOUNT_CHARGED);
    }

    public CollectPaymentEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        this.key = row.getObject(ORDER_NUMBER);
        this.accountNumber = row.getObject(ACCOUNT_NUMBER);
        this.amountCharged = row.getObject(AMOUNT_CHARGED);
        Long time = row.getObject(EVENT_TIME);
        if (time != null)
            setTimestamp(time);
    }

    @Override
    public Order apply(Order order) {
        //order.setWaitingOn(EnumSet.of(WaitingOn.CREDIT_CHECK, WaitingOn.RESERVE_INVENTORY));
        return order;
    }

    @Override
    public String toString() {
        return "CollectPaymentEvent for order " + getKey() + " amount charged " + amountCharged;
    }

    @Override
    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(getEventName())
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setString(ORDER_NUMBER, key)
                .setString(ACCOUNT_NUMBER, accountNumber)
                .setDecimal(AMOUNT_CHARGED, amountCharged)
                .build();
        return gr;
    }
}
