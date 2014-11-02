package io.github.mattthomson.depijp;

import cascading.tap.Tap;
import cascading.tuple.Fields;

public interface PijpSink<T> {
    Tap createSinkTap(Fields field);

    public default void writeTo(Pijp<T> pijp) {
        pijp.getFlowDef().addTailSink(pijp.getPipe(), createSinkTap(pijp.getField()));
    }
}
