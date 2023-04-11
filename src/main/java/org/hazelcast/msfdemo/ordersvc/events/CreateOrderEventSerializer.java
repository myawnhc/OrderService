package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import javax.annotation.Nonnull;

public class CreateOrderEventSerializer implements CompactSerializer<CreateOrderEvent> {
    @Nonnull
    @Override
    public CreateOrderEvent read(@Nonnull CompactReader compactReader) {
        String orderNumber = compactReader.readString(SourcedEvent.KEY);
        String eventClass = compactReader.readString(SourcedEvent.EVENT_CLASS);
        long timestamp = compactReader.readInt64(SourcedEvent.TIMESTAMP);
        HazelcastJsonValue payload = compactReader.readCompact(SourcedEvent.PAYLOAD);
        JSONObject jobj = new JSONObject(payload.getValue());
        String acctNumber = jobj.getString(CreateOrderEvent.ACCT_NUM);
        String itemNumber = jobj.getString(CreateOrderEvent.ITEM_NUM);
        String location = jobj.getString(CreateOrderEvent.LOCATION);
        int quantity = jobj.getInt(CreateOrderEvent.QUANTITY);
        CreateOrderEvent event = new CreateOrderEvent(orderNumber, acctNumber,
                itemNumber, location, quantity);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }

    @Override
    public void write(@Nonnull CompactWriter compactWriter, @Nonnull CreateOrderEvent createOrderEvent) {
        compactWriter.writeString(SourcedEvent.KEY, createOrderEvent.getKey());
        compactWriter.writeString(SourcedEvent.EVENT_CLASS, createOrderEvent.getEventClass());
        compactWriter.writeInt64(SourcedEvent.TIMESTAMP, createOrderEvent.getTimestamp());
        compactWriter.writeCompact(SourcedEvent.PAYLOAD, createOrderEvent.getPayload());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return CreateOrderEvent.class.getCanonicalName();
    }

    @Nonnull
    @Override
    public Class<CreateOrderEvent> getCompactClass() {
        return CreateOrderEvent.class;
    }
}
