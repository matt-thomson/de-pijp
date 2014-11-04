package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.tap.SinkTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class InMemorySinkTap<T> extends SinkTap<Properties, Void> {
    private final ListTupleEntryCollector<T> collector;
    private final String identifier;

    public InMemorySinkTap(Fields field) {
        super(new InMemoryScheme(field));

        this.collector = new ListTupleEntryCollector<>(field);
        this.identifier = String.format("in-memory-sink-tap-%s", field);
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<Properties> flowProcess, Void output) throws IOException {
        return collector;
    }

    @Override
    public boolean createResource(Properties conf) throws IOException {
        return true;
    }

    @Override
    public boolean deleteResource(Properties conf) throws IOException {
        return true;
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        return 0;
    }

    public List<T> getValues() {
        return collector.getValues();
    }
}
