package io.github.mattthomson.depijp;

import java.io.Serializable;

import cascading.tap.Tap;

public interface DePijpTap<T> extends DePijpSource<T>, DePijpSink<T>, Serializable {
    Tap createLocalTap();

    Tap createHadoopTap();

    @Override
    public default Tap createLocalSourceTap() {
        return createLocalTap();
    }

    @Override
    public default Tap createHadoopSourceTap() {
        return createHadoopTap();
    }

    @Override
    public default Tap createLocalSinkTap() {
        return createLocalTap();
    }

    @Override
    public default Tap createHadoopSinkTap() {
        return createHadoopTap();
    }
}
