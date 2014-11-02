package io.github.mattthomson.depijp;

import cascading.tap.Tap;
import cascading.tuple.Fields;

public abstract class PijpSink<T> {
    public void write(Pijp<T> pijp) {
        pijp.getFlowDef().addTailSink(pijp.getPipe(), createSinkTap(pijp.getField()));
    }

    public abstract Tap createSinkTap(Fields field);
}
