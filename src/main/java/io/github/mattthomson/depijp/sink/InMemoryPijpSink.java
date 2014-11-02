package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.cascading.InMemorySinkTap;

import java.util.List;

public class InMemoryPijpSink<T> implements PijpSink<T> {
    private InMemorySinkTap<T> tap;

    @Override
    public Tap createSinkTap(Fields field) {
        if (tap == null) {
            tap = new InMemorySinkTap<>(field);
        }

        return tap;
    }

    public List<T> getValues() {
        return tap.getValues();
    }
}
