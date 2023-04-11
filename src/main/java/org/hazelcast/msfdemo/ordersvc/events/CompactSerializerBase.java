package org.hazelcast.msfdemo.ordersvc.events;

import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;
import org.hazelcast.eventsourcing.event.SourcedEvent;

import javax.annotation.Nonnull;

public abstract class CompactSerializerBase<E extends SourcedEvent> implements CompactSerializer<E> {

    private Class<E> typeClass;

    protected CompactSerializerBase(Class<E> typeClass) {
        this.typeClass = typeClass;
    }

    @Nonnull
    @Override
    abstract public E read(@Nonnull CompactReader compactReader);

    @Override
    public void write(@Nonnull CompactWriter compactWriter, @Nonnull E event) {
        compactWriter.writeString(SourcedEvent.KEY, (String) event.getKey());
        compactWriter.writeString(SourcedEvent.EVENT_CLASS, event.getEventClass());
        compactWriter.writeInt64(SourcedEvent.TIMESTAMP, event.getTimestamp());
        compactWriter.writeCompact(SourcedEvent.PAYLOAD, event.getPayload());
    }

    @Nonnull
    @Override
    public String getTypeName() {
        return typeClass.getCanonicalName();
    }

    @Nonnull
    @Override
    public Class<E> getCompactClass() {
        return typeClass;
    }
}
