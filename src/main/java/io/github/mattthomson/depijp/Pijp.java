package io.github.mattthomson.depijp;

import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.cascading.ToTupleEntryFunction;

public class Pijp<T> {
    private final FlowDef flowDef;
    private final Pipe pipe;
    private final Fields field;

    public Pijp(FlowDef flowDef, Pipe pipe, Fields field) {
        this.flowDef = flowDef;
        this.pipe = pipe;
        this.field = field;
    }

    public void write(DePijpSink<T> sink) {
        Pipe transformed = new Each(pipe, new ToTupleEntryFunction<>(sink, field));
        flowDef.addTailSink(transformed, sink.createSinkTap());
    }
}
