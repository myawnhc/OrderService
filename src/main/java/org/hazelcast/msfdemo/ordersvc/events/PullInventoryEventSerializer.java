package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import javax.annotation.Nonnull;

public class PullInventoryEventSerializer extends CompactSerializerBase<PullInventoryEvent> {

    public PullInventoryEventSerializer() {
        super(PullInventoryEvent.class);
    }

    @Nonnull
    @Override
    public PullInventoryEvent read(@Nonnull CompactReader compactReader) {
        String orderNumber = compactReader.readString(SourcedEvent.KEY);
        String eventClass = compactReader.readString(SourcedEvent.EVENT_CLASS);
        long timestamp = compactReader.readInt64(SourcedEvent.TIMESTAMP);
        HazelcastJsonValue payload = compactReader.readCompact(SourcedEvent.PAYLOAD);
        JSONObject jobj = new JSONObject(payload.getValue());
        String acctNumber = jobj.getString(PullInventoryEvent.ACCT_NUMBER);
        String itemNumber = jobj.getString(PullInventoryEvent.ITEM_NUMBER);
        String location = jobj.getString(PullInventoryEvent.LOCATION);
        int quantity = jobj.getInt(PullInventoryEvent.QUANTITY);
        PullInventoryEvent event = new PullInventoryEvent(orderNumber, acctNumber,
                itemNumber, location, quantity);
        String failureReason = jobj.optString(PullInventoryEvent.FAILURE_REASON);
        if (failureReason != null)
            event.setFailureReason(failureReason);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }
}
