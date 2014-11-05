package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import io.github.mattthomson.depijp.DePijpException;
import io.github.mattthomson.depijp.DePijpSink;
import io.github.mattthomson.depijp.cascading.InMemorySinkTap;

import java.util.List;
import java.util.UUID;

public class InMemoryDePijpSink<T> implements DePijpSink<T> {
    private final Fields field = new Fields(UUID.randomUUID().toString());

    private InMemorySinkTap<T> tap;

    @Override
    public Tap createLocalSinkTap() {
        if (tap != null) {
            throw new DePijpException("Can't write to the same sink twice");
        }

        tap = new InMemorySinkTap<>(field);
        return tap;
    }

    @Override
    public Tap createHadoopSinkTap() {
        throw new DePijpException("In-memory sinks are not available in Hadoop mode");
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
            throw new DePijpException("Must sink before getting values");
        }

        return tap.getValues();
    }
}
