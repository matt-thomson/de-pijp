package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.tap.SourceTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class InMemorySourceTap<T> extends SourceTap<Properties, Void> {
    private final Fields field;
    private final List<T> values;
    private final String identifier;

    public InMemorySourceTap(Fields field, List<T> values) {
        super(new InMemoryScheme(field));

        this.field = field;
        this.values = values;
        this.identifier = String.format("in-memory-source-tap-%s", field);
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<Properties> flowProcess, Void input) throws IOException {
        return new ListTupleEntryIterator<>(field, values);
    }

    @Override
    public boolean resourceExists(Properties conf) throws IOException {
        return true;
    }

    @Override
    public long getModifiedTime(Properties conf) throws IOException {
        return 0;
    }
}
