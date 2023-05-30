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

public class PullInventoryEvent extends OrderEvent {

    public static final String ACCT_NUMBER = "accountNumber";
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String QUANTITY = "quantity";
    public static final String LOCATION = "location";
    public static final String FAILURE_REASON = "failureReason";

    public PullInventoryEvent(String orderNumber, String acctNumber, String itemNumber, String location, int quantity) {
        this.key = orderNumber;
        this.eventClass = PullInventoryEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put(ACCT_NUMBER, acctNumber);
        jobj.put(ITEM_NUMBER, itemNumber);
        jobj.put(LOCATION, location);
        jobj.put(QUANTITY, quantity);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public void setFailureReason(String reason) {
        JSONObject jobj = new JSONObject(payload.getValue());
        jobj.put(FAILURE_REASON, reason);
        payload = new HazelcastJsonValue(jobj.toString());
    }

    public PullInventoryEvent(SqlRow row) {
        this.key = row.getObject("key");
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        eventClass = PullInventoryEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
    }

    public Order apply(Order order) {
        // May in some future implementation alter quantity, for partial ship,
        // but we're not doing that currently.
        JSONObject jobj = new JSONObject(payload.getValue());
        order.setQuantity(jobj.getInt(QUANTITY));

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
        JSONObject jobj = new JSONObject(payload.getValue());
        return "PullInventoryEvent I:" + jobj.getString(ITEM_NUMBER) + " Q:" + jobj.getInt(QUANTITY);
    }
}


