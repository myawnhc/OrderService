package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.msfdemo.ordersvc.domain.Order;

import java.util.EnumSet;

public class CreateOrderEvent extends OrderEvent {

    public static final String ACCT_NUM = "acctNumber";
    public static final String ITEM_NUM = "itemNumber";
    public static final String LOCATION = "location";
    public static final String QUANTITY = "quantity";

    public CreateOrderEvent(String orderNumber, String acctNumber, String itemNumber,
                            String location, int quantity) {
        this.key = orderNumber;
        this.eventClass = CreateOrderEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put(ACCT_NUM, acctNumber);
        jobj.put(ITEM_NUM, itemNumber);
        jobj.put(LOCATION, location);
        jobj.put(QUANTITY, quantity);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public CreateOrderEvent(SqlRow row) {
        this.key = row.getObject("key");
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        eventClass = CreateOrderEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
    }

    @Override
    public Order apply(Order order) {
        if (order == null)
            order = new Order();
        order.setOrderNumber(getKey());
        JSONObject jobj = new JSONObject(payload.getValue());
        order.setAcctNumber(jobj.getString(ACCT_NUM));
        order.setItemNumber(jobj.getString(ITEM_NUM));
        order.setLocation(jobj.getString(LOCATION));
        order.setQuantity(jobj.getInt(QUANTITY));
        //order.setWaitingOn(EnumSet.of(WaitingOn.PRICE_LOOKUP));
        return order;
    }

    @Override
    public String toString() {
        JSONObject jobj = new JSONObject(payload.getValue());
        return "CreateOrderEvent for order " + getKey() + " I:" + jobj.getString(ITEM_NUM) +
                "L:" + jobj.getString(LOCATION) +
                "A:" + jobj.getString(ACCT_NUM) +
                "Q:" + jobj.getInt(QUANTITY);
    }
}
