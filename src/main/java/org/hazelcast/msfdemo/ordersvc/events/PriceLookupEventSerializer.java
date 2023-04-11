package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import javax.annotation.Nonnull;

public class PriceLookupEventSerializer implements CompactSerializer<PriceLookupEvent> {
    @Nonnull
    @Override
    public PriceLookupEvent read(@Nonnull CompactReader compactReader) {
        String orderNumber = compactReader.readString(SourcedEvent.KEY);
        String eventClass = compactReader.readString(SourcedEvent.EVENT_CLASS);
        long timestamp = compactReader.readInt64(SourcedEvent.TIMESTAMP);
        HazelcastJsonValue payload = compactReader.readCompact(SourcedEvent.PAYLOAD);
        JSONObject jobj = new JSONObject(payload.getValue());
        String itemNumber = jobj.getString(PriceLookupEvent.ITEM_NUMBER);
        String location = jobj.getString(PriceLookupEvent.LOCATION);
        int quantity = jobj.getInt(PriceLookupEvent.QUANTITY);
        int price = jobj.getInt(PriceLookupEvent.EXTENDED_PRICE);
        PriceLookupEvent event = new PriceLookupEvent(orderNumber,
                itemNumber, location, quantity, price);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }

    @Override
    public void write(@Nonnull CompactWriter compactWriter, @Nonnull PriceLookupEvent priceLookupEvent) {
        compactWriter.writeString(SourcedEvent.KEY, priceLookupEvent.getKey());
        compactWriter.writeString(SourcedEvent.EVENT_CLASS, priceLookupEvent.getEventClass());
        compactWriter.writeInt64(SourcedEvent.TIMESTAMP, priceLookupEvent.getTimestamp());
        compactWriter.writeCompact(SourcedEvent.PAYLOAD, priceLookupEvent.getPayload());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return PriceLookupEvent.class.getCanonicalName();
    }

    @Nonnull
    @Override
    public Class<PriceLookupEvent> getCompactClass() {
        return PriceLookupEvent.class;
    }
}
