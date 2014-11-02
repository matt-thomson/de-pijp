package io.github.mattthomson.depijp.source;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.collect.ImmutableList;
import io.github.mattthomson.depijp.cascading.InMemorySourceTap;

import java.util.List;

public class InMemoryPijpSource<T> implements PijpSource<T> {
    private final List<T> values;

    @SafeVarargs
    public InMemoryPijpSource(T... values) {
        this.values = ImmutableList.copyOf(values);
    }

    @Override
    public Tap createSourceTap(Fields field) {
        return new InMemorySourceTap<>(field, values);
    }
}
