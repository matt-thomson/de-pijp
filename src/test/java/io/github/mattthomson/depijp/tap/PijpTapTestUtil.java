package io.github.mattthomson.depijp.tap;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import io.github.mattthomson.depijp.PijpSink;
import io.github.mattthomson.depijp.PijpSource;
import io.github.mattthomson.depijp.sink.InMemoryPijpSink;
import io.github.mattthomson.depijp.source.InMemoryPijpSource;

import java.util.List;

public final class PijpTapTestUtil {
    private PijpTapTestUtil() {
        // blank to prevent instantiation
    }

    static <T> List<T> readFromSource(PijpSource<T> source) {
        InMemoryPijpSink<T> sink = new InMemoryPijpSink<>();
        runFlow(source, sink);
        return sink.getValues();
    }

    @SafeVarargs
    static <T> void writeToSink(PijpSink<T> sink, T... values) {
        PijpSource<T> source = new InMemoryPijpSource<>(values);
        runFlow(source, sink);
    }

    private static <T> void runFlow(PijpSource<T> source, PijpSink<T> sink) {
        FlowDef flowDef = new FlowDef();
        sink.writeTo(source.readFrom(flowDef));
        new LocalFlowConnector().connect(flowDef).complete();
    }
}
