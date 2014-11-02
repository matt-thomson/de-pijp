package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.PijpException;
import io.github.mattthomson.depijp.PijpSink;
import io.github.mattthomson.depijp.cascading.InMemorySinkTap;

import java.util.List;

public class InMemoryPijpSink<T> extends PijpSink<T> {
    private InMemorySinkTap<T> tap;

    @Override
    public Tap createSinkTap(Fields field) {
        if (tap != null) {
            throw new PijpException("Can't write to the same sink twice");
        }

        tap = new InMemorySinkTap<>(field);
        return tap;
    }

    public List<T> getValues() {
        if (tap == null) {
            throw new PijpException("Must sink before getting values");
        }

        return tap.getValues();
    }
}
