package io.github.mattthomson.depijp.mode;

import cascading.flow.FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.tap.Tap;
import io.github.mattthomson.depijp.DePijpSink;
import io.github.mattthomson.depijp.DePijpSource;

public class LocalDePijpMode implements DePijpMode {
    @Override
    public FlowConnector createFlowConnector() {
        return new LocalFlowConnector();
    }

    @Override
    public <T> Tap createSourceTap(DePijpSource<T> source) {
        return source.createLocalSourceTap();
    }

    @Override
    public <T> Tap createSinkTap(DePijpSink<T> sink) {
        return sink.createLocalSinkTap();
    }
}
