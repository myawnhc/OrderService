package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.org.json.JSONObject;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.msfdemo.ordersvc.domain.Order;

import java.io.Serializable;
import java.util.function.UnaryOperator;

public class ShipOrderEvent extends OrderEvent implements Serializable, UnaryOperator<Order>
{
    public static final String ORDER_NUMBER = "orderNumber";
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String FROM_LOCATION = "location";
    public static final String QUANTITY = "quantity";

    public ShipOrderEvent(String orderNumber, String itemNumber,
                            String fromLocation, int quantity) {
        this.key = orderNumber;
        this.eventClass = CreditCheckEvent.class.getCanonicalName();
        JSONObject jobj = new JSONObject();
        jobj.put(ORDER_NUMBER, orderNumber);
        jobj.put(ITEM_NUMBER, itemNumber);
        jobj.put(FROM_LOCATION, fromLocation);
        jobj.put(QUANTITY, quantity);
        setPayload(new HazelcastJsonValue(jobj.toString()));
    }

    public ShipOrderEvent(SqlRow row) {
        this.key = row.getObject("key");
        HazelcastJsonValue payload = row.getObject("payload");
        setPayload(payload);
        eventClass = ShipOrderEvent.class.getCanonicalName();
        setTimestamp(row.getObject("timestamp"));
    }

    @Override
    public Order apply(Order order) {
        //System.out.println("*** ShipOrderEvent.apply is a nop - this may be OK ***");
        return order;
    }
}
