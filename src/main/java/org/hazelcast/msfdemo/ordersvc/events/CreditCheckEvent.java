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
import java.io.Serializable;
import java.util.function.UnaryOperator;

import static org.hazelcast.msfdemo.ordersvc.events.OrderOuterClass.CreditChecked;

public class CreditCheckEvent extends OrderEvent implements Serializable, UnaryOperator<Order> {

    public static final String ACCT_NUMBER = "acccountNumber";
    public static final String AMT_REQUESTED = "amountRequested";
    public static final String APPROVED = "approved";


    public CreditCheckEvent(String orderNumber, String acctNumber,
                            int amountRequested, boolean approved) {
        this.key = orderNumber;
        this.eventClass = CreditCheckEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put(ACCT_NUMBER, acctNumber);
        jobj.put(AMT_REQUESTED, amountRequested);
        jobj.put(APPROVED, approved);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public CreditCheckEvent(SqlRow row) {
        this.key = row.getObject("key");
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        eventClass = CreditCheckEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
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
        System.out.println("*** CreditCheckEvent.apply is a nop - this may be OK ***");
        return order;
    }
}
