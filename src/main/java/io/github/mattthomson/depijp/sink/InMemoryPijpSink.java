package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import io.github.mattthomson.depijp.PijpException;
import io.github.mattthomson.depijp.PijpSink;
import io.github.mattthomson.depijp.cascading.InMemorySinkTap;

import java.util.List;
import java.util.UUID;

public class InMemoryPijpSink<T> implements PijpSink<T> {
    private final Fields field = new Fields(UUID.randomUUID().toString());

    private InMemorySinkTap<T> tap;

    @Override
    public Tap createSinkTap() {
        if (tap != null) {
            throw new PijpException("Can't write to the same sink twice");
        }

        tap = new InMemorySinkTap<>(field);
        return tap;
    }

    @Override
    public Tuple toTuple(T value) {
        return new Tuple(value);
    }

    @Override
    public Fields getOutputFields() {
        return field;
    }

    public List<T> getValues() {
        if (tap == null) {
            throw new PijpException("Must sink before getting values");
        }

        return tap.getValues();
    }
}
