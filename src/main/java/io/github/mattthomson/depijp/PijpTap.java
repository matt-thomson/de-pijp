package io.github.mattthomson.depijp;

import cascading.tap.Tap;
import cascading.tuple.Fields;

public interface PijpTap<T> extends PijpSource<T>, PijpSink<T> {
    Tap createTap(Fields field);

    @Override
    public default Tap createSourceTap(Fields field) {
        return createTap(field);
    }

    @Override
    public default Tap createSinkTap(Fields field) {
        return createTap(field);
    }
}
