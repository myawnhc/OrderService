package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.msfdemo.ordersvc.domain.Order;

import java.util.EnumSet;

public class CreateOrderEvent extends OrderEvent {

    public static final String QUAL_EVENT_NAME = "OrderService:CreateOrderEvent";
    public static final String ORDER_NUMBER = "doKey";
    public static final String ACCT_NUM = "accountNumber";
    public static final String ITEM_NUM = "itemNumber";
    public static final String LOCATION = "location";
    public static final String QUANTITY = "quantity";

    String accountNumber;
    String itemNumber;
    String location;
    int quantity;

    public CreateOrderEvent(String orderNumber, String acctNumber, String itemNumber,
                            String location, int quantity) {
        setEventName(QUAL_EVENT_NAME);
        this.key = orderNumber;
        this.accountNumber = acctNumber;
        this.itemNumber = itemNumber;
        this.location = location;
        this.quantity = quantity;
    }

    public CreateOrderEvent(GenericRecord data) {
        setEventName(QUAL_EVENT_NAME);
        this.key = data.getString(ORDER_NUMBER);
        this.accountNumber = data.getString(ACCT_NUM);
        this.itemNumber = data.getString(ITEM_NUM);
        this.location = data.getString(LOCATION);
        this.quantity = data.getInt32(QUANTITY);
    }

    public CreateOrderEvent(SqlRow row) {
        setEventName(QUAL_EVENT_NAME);
        this.key = row.getObject("key");
        this.accountNumber = row.getObject(ACCT_NUM);
        this.itemNumber = row.getObject(ITEM_NUM);
        this.location = row.getObject(LOCATION);
        this.quantity = row.getObject(QUANTITY);
        Long time = row.getObject(EVENT_TIME);
        if (time != null)
            setTimestamp(time);
    }

    public String getAccountNumber() { return accountNumber; }
    public String getItemNumber() { return itemNumber; }
    public String getLocation() { return location; }
    public int getQuantity() { return quantity; }

    @Override
    public Order apply(Order order) {
        if (order == null)
            order = new Order();
        order.setOrderNumber(getKey());
        order.setAcctNumber(accountNumber);
        order.setItemNumber(itemNumber);
        order.setLocation(location);
        order.setQuantity(quantity);
        //order.setWaitingOn(EnumSet.of(WaitingOn.PRICE_LOOKUP));
        return order;
    }

    @Override
    public String toString() {
        return "CreateOrderEvent for order " + getKey() +
                " I:" + itemNumber +
                " L:" + location +
                " A:" + accountNumber +
                " Q:" + quantity;
    }

    @Override
    public GenericRecord toGenericRecord() {
        GenericRecord gr = GenericRecordBuilder.compact(getEventName())
                .setString(EVENT_NAME, QUAL_EVENT_NAME)
                .setString(ORDER_NUMBER, getOrderNumber())
                .setString(ITEM_NUM, itemNumber)
                .setString(ACCT_NUM, accountNumber)
                .setString(LOCATION, location)
                .setInt32(QUANTITY, quantity)
                .build();
        return gr;
    }
}
