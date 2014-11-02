package io.github.mattthomson.depijp;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.sink.PijpSink;

public class Pijp<T> {
    private final FlowDef flowDef;
    private final Pipe pipe;
    private final Fields field;

    Pijp(FlowDef flowDef, Pipe pipe, Fields field) {
        this.flowDef = flowDef;
        this.pipe = pipe;
        this.field = field;
    }

    public void write(PijpSink<T> sink) {
        flowDef.addTailSink(pipe, sink.createSinkTap(field));
    }
}
