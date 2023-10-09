package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.msfdemo.ordersvc.domain.Order;

import java.io.Serializable;
import java.util.function.UnaryOperator;

public class ShipOrderEvent extends OrderEvent implements Serializable, UnaryOperator<Order>
{
    public static final String QUAL_EVENT_NAME = "OrderService:ShipOrderEvent";
    public static final String ORDER_NUMBER = "doKey";
    public static final String ITEM_NUMBER = "itemNumber";
    public static final String FROM_LOCATION = "location";
    public static final String QUANTITY = "quantity";

    private String itemNumber;
    private String location;
    private int quantity;

    public ShipOrderEvent(String orderNumber, String itemNumber,
                            String fromLocation, int quantity) {
        setEventName(QUAL_EVENT_NAME);
        this.key = orderNumber;
        this.itemNumber = itemNumber;
        this.location = fromLocation;
        this.quantity = quantity;
    }

    public ShipOrderEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.key = data.getString(ORDER_NUMBER);
        this.itemNumber = data.getString(ITEM_NUMBER);
        this.location = data.getString(FROM_LOCATION);
        this.quantity = data.getInt32(QUANTITY);
    }

    public ShipOrderEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        this.key = row.getObject("key");
        this.itemNumber = row.getObject(ITEM_NUMBER);
        this.location = row.getObject(FROM_LOCATION);
        this.quantity = row.getObject(QUANTITY);
        Long time = row.getObject(EVENT_TIME);
        if (time != null)
            setTimestamp(time);
    }

    @Override
    public Order apply(Order order) {
        //System.out.println("*** ShipOrderEvent.apply is a nop - this may be OK ***");
        return order;
    }

    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(getEventName())
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setString(ORDER_NUMBER, getOrderNumber())
                .setString(ITEM_NUMBER, itemNumber)
                .setString(FROM_LOCATION, location)
                .setInt32(QUANTITY, quantity)
                .build();
        return gr;
    }
}
