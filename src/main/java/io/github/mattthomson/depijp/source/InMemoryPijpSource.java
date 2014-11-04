package io.github.mattthomson.depijp.source;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import com.google.common.collect.ImmutableList;
import io.github.mattthomson.depijp.PijpSource;
import io.github.mattthomson.depijp.cascading.InMemorySourceTap;

import java.util.List;
import java.util.UUID;

public class InMemoryPijpSource<T> implements PijpSource<T> {
    private final List<T> values;
    private final Fields field = new Fields(UUID.randomUUID().toString());

    @SafeVarargs
    public InMemoryPijpSource(T... values) {
        this.values = ImmutableList.copyOf(values);
    }

    @Override
    public Tap createSourceTap() {
        return new InMemorySourceTap<>(field, values);
    }

    @Override
    public T fromTupleEntry(TupleEntry tupleEntry) {
        return (T) tupleEntry.getObject(field);
    }
}
