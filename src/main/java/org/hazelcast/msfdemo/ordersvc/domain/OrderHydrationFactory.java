package org.hazelcast.msfdemo.ordersvc.domain;

import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.sql.SqlRow;
import org.hazelcast.eventsourcing.event.HydrationFactory;
import org.hazelcast.msfdemo.ordersvc.events.CollectPaymentEvent;
import org.hazelcast.msfdemo.ordersvc.events.CreateOrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.CreditCheckEvent;
import org.hazelcast.msfdemo.ordersvc.events.OrderEvent;
import org.hazelcast.msfdemo.ordersvc.events.PriceLookupEvent;
import org.hazelcast.msfdemo.ordersvc.events.PullInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.events.ReserveInventoryEvent;
import org.hazelcast.msfdemo.ordersvc.events.ShipOrderEvent;

import java.io.Serializable;

public class OrderHydrationFactory
    implements HydrationFactory<Order, String, OrderEvent>, Serializable {

    //public static final String DO_NAME = Order.QUAL_DO_NAME
    private String mapping_template = "CREATE MAPPING IF NOT EXISTS \"?\" (\n" +
            //  PartitionedSequenceKey  fields
            //  domainObjectKey will be cast as VARCHAR
            //  in query which may break if domain object key isn't a String
            "   doKey        OBJECT    EXTERNAL NAME \"__key.domainObjectKey\",\n" +
            "   sequence     BIGINT    EXTERNAL NAME \"__key.sequence\",\n" +

            // SourcedEvent fields
            "   eventName    VARCHAR,\n" +
            "   eventTime    BIGINT,\n"    +

            //  OrderEvent fields
            "   itemNumber      VARCHAR,\n"  +
            "   location        VARCHAR,\n"  +
            "   accountNumber   VARCHAR,\n"  +
            "   amount          DECIMAL,\n"  +  // CollectPayment amountCharged, PriceLookup, CreditCheck amtRequested
            "   quantity        INTEGER,\n," +
            "   approved        BOOLEAN,\n," + // CreditCheck
            "   description     VARCHAR\n"   + // Failure reason on inv events
            ")\n" +
            "TYPE IMap\n" +
            "OPTIONS (\n" +
            "  'keyFormat' = 'java',\n" +
            "  'keyJavaClass' = 'org.hazelcast.eventsourcing.event.PartitionedSequenceKey',\n" +
            "  'valueFormat' = 'compact',\n" +
            "  'valueCompactTypeName' = 'OrderService:OrderEvent'\n" +
            ")";

    @Override
    public Order hydrateDomainObject(GenericRecord genericRecord) {
        return new Order(genericRecord);
    }

    @Override
    public OrderEvent hydrateEvent(String eventName, SqlRow data) {
        try {
            switch (eventName) {
                case CreateOrderEvent.QUAL_EVENT_NAME:
                    return new CreateOrderEvent(data);
                case PriceLookupEvent.QUAL_EVENT_NAME:
                    return new PriceLookupEvent(data);
                case CreditCheckEvent.QUAL_EVENT_NAME:
                    return new CreditCheckEvent(data);
                case ReserveInventoryEvent.QUAL_EVENT_NAME:
                    return new ReserveInventoryEvent(data);
                case CollectPaymentEvent.QUAL_EVENT_NAME:
                    return new CollectPaymentEvent(data);
                case PullInventoryEvent.QUAL_EVENT_NAME:
                    return new PullInventoryEvent(data);
                case ShipOrderEvent.QUAL_EVENT_NAME:
                    return new ShipOrderEvent(data);
                // future - have order compaction event here
                default:
                    throw new IllegalArgumentException("bad eventName: " + eventName);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public OrderEvent hydrateEvent(String eventName, GenericRecord data) {
        try {
            switch (eventName) {
                case CreateOrderEvent.QUAL_EVENT_NAME:
                    return new CreateOrderEvent(data);
                case PriceLookupEvent.QUAL_EVENT_NAME:
                    return new PriceLookupEvent(data);
                case CreditCheckEvent.QUAL_EVENT_NAME:
                    return new CreditCheckEvent(data);
                case ReserveInventoryEvent.QUAL_EVENT_NAME:
                    return new ReserveInventoryEvent(data);
                case CollectPaymentEvent.QUAL_EVENT_NAME:
                    return new CollectPaymentEvent(data);
                case PullInventoryEvent.QUAL_EVENT_NAME:
                    return new PullInventoryEvent(data);
                case ShipOrderEvent.QUAL_EVENT_NAME:
                    return new ShipOrderEvent(data);
                // future - have order compaction event here
                default:
                    throw new IllegalArgumentException("bad eventName: " + eventName);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String getEventMapping(String eventStoreName) {
        mapping_template = mapping_template.replaceAll("\\?", eventStoreName);
        return mapping_template;
    }
}
