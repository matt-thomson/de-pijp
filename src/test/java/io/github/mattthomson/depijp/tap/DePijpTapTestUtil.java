package io.github.mattthomson.depijp.tap;

import io.github.mattthomson.depijp.PijpBuilder;
import io.github.mattthomson.depijp.DePijpSink;
import io.github.mattthomson.depijp.DePijpSource;
import io.github.mattthomson.depijp.sink.InMemoryDePijpSink;
import io.github.mattthomson.depijp.source.InMemoryDePijpSource;

import java.util.List;

public final class DePijpTapTestUtil {
    private DePijpTapTestUtil() {
        // blank to prevent instantiation
    }

    static <T> List<T> readFromSource(DePijpSource<T> source) {
        InMemoryDePijpSink<T> sink = new InMemoryDePijpSink<>();
        runFlow(source, sink);
        return sink.getValues();
    }

    @SafeVarargs
    static <T> void writeToSink(DePijpSink<T> sink, T... values) {
        DePijpSource<T> source = new InMemoryDePijpSource<>(values);
        runFlow(source, sink);
    }

    private static <T> void runFlow(DePijpSource<T> source, DePijpSink<T> sink) {
        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).write(sink);
        pijpBuilder.run();
    }
}
