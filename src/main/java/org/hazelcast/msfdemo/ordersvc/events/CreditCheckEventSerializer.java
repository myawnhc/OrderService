package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import javax.annotation.Nonnull;

public class CreditCheckEventSerializer implements CompactSerializer<CreditCheckEvent> {
    @Nonnull
    @Override
    public CreditCheckEvent read(@Nonnull CompactReader compactReader) {
        String orderNumber = compactReader.readString(SourcedEvent.KEY);
        String eventClass = compactReader.readString(SourcedEvent.EVENT_CLASS);
        long timestamp = compactReader.readInt64(SourcedEvent.TIMESTAMP);
        HazelcastJsonValue payload = compactReader.readCompact(SourcedEvent.PAYLOAD);
        JSONObject jobj = new JSONObject(payload.getValue());
        String acctNumber = jobj.getString(CreditCheckEvent.ACCT_NUMBER);
        int amountRequested = jobj.getInt(CreditCheckEvent.AMT_REQUESTED);
        boolean approved = jobj.getBoolean(CreditCheckEvent.APPROVED);
        CreditCheckEvent event = new CreditCheckEvent(orderNumber, acctNumber,
                amountRequested, approved);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }

    @Override
    public void write(@Nonnull CompactWriter compactWriter, @Nonnull CreditCheckEvent creditCheckEvent) {
        compactWriter.writeString(SourcedEvent.KEY, creditCheckEvent.getKey());
        compactWriter.writeString(SourcedEvent.EVENT_CLASS, creditCheckEvent.getEventClass());
        compactWriter.writeInt64(SourcedEvent.TIMESTAMP, creditCheckEvent.getTimestamp());
        compactWriter.writeCompact(SourcedEvent.PAYLOAD, creditCheckEvent.getPayload());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return CreditCheckEvent.class.getCanonicalName();
    }

    @Nonnull
    @Override
    public Class<CreditCheckEvent> getCompactClass() {
        return CreditCheckEvent.class;
    }
}
