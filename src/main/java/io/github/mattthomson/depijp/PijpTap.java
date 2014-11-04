package io.github.mattthomson.depijp;

import cascading.tap.Tap;

public interface PijpTap<T> extends PijpSource<T>, PijpSink<T> {
    Tap createTap();

    @Override
    public default Tap createSourceTap() {
        return createTap();
    }

    @Override
    public default Tap createSinkTap() {
        return createTap();
    }
}
