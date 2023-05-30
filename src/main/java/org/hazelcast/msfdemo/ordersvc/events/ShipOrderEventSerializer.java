package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.org.json.JSONObject;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import javax.annotation.Nonnull;

public class ShipOrderEventSerializer extends CompactSerializerBase<ShipOrderEvent> {

    public ShipOrderEventSerializer() {
        super(ShipOrderEvent.class);
    }

    @Nonnull
    @Override
    public ShipOrderEvent read(@Nonnull CompactReader compactReader) {
        String orderNumber = compactReader.readString(SourcedEvent.KEY);
        String eventClass = compactReader.readString(SourcedEvent.EVENT_CLASS);
        long timestamp = compactReader.readInt64(SourcedEvent.TIMESTAMP);
        HazelcastJsonValue payload = compactReader.readCompact(SourcedEvent.PAYLOAD);
        JSONObject jobj = new JSONObject(payload.getValue());
        String itemNumber = jobj.getString(ShipOrderEvent.ITEM_NUMBER);
        String location = jobj.getString(ShipOrderEvent.FROM_LOCATION);
        int quantity = jobj.getInt(ShipOrderEvent.QUANTITY);
        ShipOrderEvent event = new ShipOrderEvent(orderNumber,
                itemNumber, location, quantity);
//        String failureReason = jobj.optString(ShipOrderEvent.FAILURE_REASON);
//        if (failureReason != null)
//            event.setFailureReason(failureReason);
        event.setTimestamp(timestamp);
        event.setEventClass(eventClass);
        return event;
    }
}
