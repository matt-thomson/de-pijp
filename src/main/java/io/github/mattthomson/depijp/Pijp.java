package io.github.mattthomson.depijp;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class Pijp<T> {
    private final FlowDef flowDef;
    private final Pipe pipe;
    private final Fields field;

    public Pijp(FlowDef flowDef, Pipe pipe, Fields field) {
        this.flowDef = flowDef;
        this.pipe = pipe;
        this.field = field;
    }

    public void write(PijpSink<T> sink) {
        sink.write(this);
    }

    FlowDef getFlowDef() {
        return flowDef;
    }

    Pipe getPipe() {
        return pipe;
    }

    Fields getField() {
        return field;
    }
}
