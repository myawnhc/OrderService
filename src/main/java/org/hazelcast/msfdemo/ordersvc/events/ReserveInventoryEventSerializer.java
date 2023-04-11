package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import javax.annotation.Nonnull;

public class ReserveInventoryEventSerializer extends CompactSerializerBase<ReserveInventoryEvent> {

    public ReserveInventoryEventSerializer() {
        super(ReserveInventoryEvent.class);
    }

    @Nonnull
    @Override
    public ReserveInventoryEvent read(@Nonnull CompactReader compactReader) {
        String orderNumber = compactReader.readString(SourcedEvent.KEY);
        String eventClass = compactReader.readString(SourcedEvent.EVENT_CLASS);
        long timestamp = compactReader.readInt64(SourcedEvent.TIMESTAMP);
        HazelcastJsonValue payload = compactReader.readCompact(SourcedEvent.PAYLOAD);
        JSONObject jobj = new JSONObject(payload.getValue());
        String acctNumber = jobj.getString(ReserveInventoryEvent.ACCT_NUMBER);
        String itemNumber = jobj.getString(ReserveInventoryEvent.ITEM_NUMBER);
        String location = jobj.getString(ReserveInventoryEvent.LOCATION);
        int quantity = jobj.getInt(ReserveInventoryEvent.QUANTITY);
        ReserveInventoryEvent event = new ReserveInventoryEvent(orderNumber, acctNumber,
                itemNumber, location, quantity);
        String failureReason = jobj.optString(ReserveInventoryEvent.FAILURE_REASON);
        if (failureReason != null)
            event.setFailureReason(failureReason);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }
}
