package io.github.mattthomson.depijp;

import cascading.flow.FlowDef;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.UUID;

public abstract class PijpSource<T> {
    public Pijp<T> read(FlowDef flowDef) {
        Pipe pipe = new Pipe(UUID.randomUUID().toString());
        Fields field = new Fields(UUID.randomUUID().toString());

        flowDef.addSource(pipe, createSourceTap(field));
        return new Pijp<>(flowDef, pipe, field);
    }

    public abstract Tap createSourceTap(Fields field);
}
