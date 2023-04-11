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

public class PriceLookupEvent extends OrderEvent {

    public static final String EXTENDED_PRICE = "extendedPrice";
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String LOCATION = "location";
    public static final String QUANTITY = "quantity";

    public PriceLookupEvent(String orderNumber, String itemNumber, String location, int quantity, int price) {
        this.key = orderNumber;
        this.eventClass = PriceLookupEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put(ITEM_NUMBER, itemNumber);
        jobj.put(LOCATION, location);
        jobj.put(QUANTITY, quantity);
        jobj.put(EXTENDED_PRICE, price);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public PriceLookupEvent(SqlRow row) {
        this.key = row.getObject("key");
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        eventClass = PriceLookupEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
    }

    @Override
    public Order apply(Order order) {
        order.setOrderNumber(getKey());
        JSONObject jobj = new JSONObject(payload.getValue());
        order.setItemNumber(jobj.getString(ITEM_NUMBER));
        order.setLocation(jobj.getString(LOCATION));
        order.setQuantity(jobj.getInt(QUANTITY));
        order.setExtendedPrice(jobj.getInt(EXTENDED_PRICE));
        //order.setWaitingOn(EnumSet.of(WaitingOn.CREDIT_CHECK, WaitingOn.RESERVE_INVENTORY));
        return order;
    }

    @Override
    public String toString() {
        JSONObject jobj = new JSONObject(payload.getValue());
        return "PriceLookupEvent for order " + getKey() + " extended price " + jobj.getInt(EXTENDED_PRICE);
    }
}
