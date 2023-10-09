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
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.function.UnaryOperator;

import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.CreditChecked;

public class CreditCheckEvent extends OrderEvent implements Serializable, UnaryOperator<Order> {

    public static final String QUAL_EVENT_NAME = "OrderService:CreditCheckEvent";
    public static final String ORDER_NUMBER = "doKey";
    public static final String ACCT_NUMBER = "accountNumber";
    public static final String AMT_REQUESTED = "amount";
    public static final String APPROVED = "approved";

    public String accountNumber;
    public BigDecimal amountRequested;
    public boolean approved;

    public CreditCheckEvent(String orderNumber, String acctNumber,
                            BigDecimal amountRequested, boolean approved) {
        setEventName(QUAL_EVENT_NAME);
        this.key = orderNumber;
        this.accountNumber = acctNumber;
        this.amountRequested = amountRequested;
        this.approved = approved;
    }

    public CreditCheckEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.key = data.getString(ORDER_NUMBER);
        this.accountNumber = data.getString(ACCT_NUMBER);
        this.amountRequested = data.getDecimal(AMT_REQUESTED);
        this.approved = data.getBoolean(APPROVED);
    }

    public CreditCheckEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        this.key = row.getObject("key");
        this.accountNumber = row.getObject(ACCT_NUMBER);
        this.amountRequested = row.getObject(AMT_REQUESTED);
        this.approved = row.getObject(APPROVED);
        Long time = row.getObject(EVENT_TIME);
        if (time != null)
            setTimestamp(time);
    }

    @Override
    public Order apply(Order order) {
//        EnumSet<WaitingOn> waits = order.getWaitingOn();
//        waits.remove(WaitingOn.CREDIT_CHECK);
//        if (waits.isEmpty()) {
//            waits.add(WaitingOn.CHARGE_ACCOUNT);
//            waits.add(WaitingOn.PULL_INVENTORY);
//        }
//        return order;
        //System.out.println("*** CreditCheckEvent.apply is a nop - this may be OK ***");
        return order;
    }

    @Override
    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(getEventName())
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setString(ORDER_NUMBER, getOrderNumber())
                .setString(ACCT_NUMBER, accountNumber)
                .setDecimal(AMT_REQUESTED, amountRequested)
                .setBoolean(APPROVED, approved)
                .build();
        return gr;
    }
}
