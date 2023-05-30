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

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.msfdemo.ordersvc.domain.Order;

import java.math.BigDecimal;

public class CollectPaymentEvent extends OrderEvent {

    public static final String ACCOUNT_NUMBER = "accountNumber";
    public static final String AMOUNT_CHARGED = "amountCharged";
    public static final String ORDER_NUMBER = "orderNumber";

    public CollectPaymentEvent(String orderNumber, String accountNumber, int amountCharged) {
        this.key = orderNumber;
        this.eventClass = CollectPaymentEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put(ORDER_NUMBER, orderNumber);
        jobj.put(ACCOUNT_NUMBER, accountNumber);
        jobj.put(AMOUNT_CHARGED, amountCharged);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public CollectPaymentEvent(SqlRow row) {
        this.key = row.getObject("key");
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        eventClass = CollectPaymentEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
    }

    @Override
    public Order apply(Order order) {
        //order.setWaitingOn(EnumSet.of(WaitingOn.CREDIT_CHECK, WaitingOn.RESERVE_INVENTORY));
        return order;
    }

    @Override
    public String toString() {
        JSONObject jobj = new JSONObject(payload.getValue());
        return "CollectPaymentEvent for order " + getKey() + " amount charged " + jobj.getInt(AMOUNT_CHARGED);
    }
}
