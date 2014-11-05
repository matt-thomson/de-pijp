package io.github.mattthomson.depijp;

import cascading.tap.Tap;

public interface DePijpTap<T> extends DePijpSource<T>, DePijpSink<T> {
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
