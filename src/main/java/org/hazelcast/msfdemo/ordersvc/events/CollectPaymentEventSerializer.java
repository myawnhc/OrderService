package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import javax.annotation.Nonnull;

public class CollectPaymentEventSerializer implements CompactSerializer<CollectPaymentEvent> {
    @Nonnull
    @Override
    public CollectPaymentEvent read(@Nonnull CompactReader compactReader) {
        String orderNumber = compactReader.readString(SourcedEvent.KEY);
        String eventClass = compactReader.readString(SourcedEvent.EVENT_CLASS);
        long timestamp = compactReader.readInt64(SourcedEvent.TIMESTAMP);
        HazelcastJsonValue payload = compactReader.readCompact(SourcedEvent.PAYLOAD);
        JSONObject jobj = new JSONObject(payload.getValue());
        String acctNumber = jobj.getString(CollectPaymentEvent.ACCOUNT_NUMBER);
        int amtCharged = jobj.getInt(CollectPaymentEvent.AMOUNT_CHARGED);
        CollectPaymentEvent event = new CollectPaymentEvent(orderNumber, acctNumber,
                amtCharged);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }

    @Override
    public void write(@Nonnull CompactWriter compactWriter, @Nonnull CollectPaymentEvent collectPaymentEvent) {
        compactWriter.writeString(SourcedEvent.KEY, collectPaymentEvent.getKey());
        compactWriter.writeString(SourcedEvent.EVENT_CLASS, collectPaymentEvent.getEventClass());
        compactWriter.writeInt64(SourcedEvent.TIMESTAMP, collectPaymentEvent.getTimestamp());
        compactWriter.writeCompact(SourcedEvent.PAYLOAD, collectPaymentEvent.getPayload());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return CollectPaymentEvent.class.getCanonicalName();
    }

    @Nonnull
    @Override
    public Class<CollectPaymentEvent> getCompactClass() {
        return CollectPaymentEvent.class;
    }
}
