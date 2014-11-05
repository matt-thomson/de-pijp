package io.github.mattthomson.depijp.mode;

import cascading.flow.FlowConnector;
import cascading.tap.Tap;
import io.github.mattthomson.depijp.DePijpSink;
import io.github.mattthomson.depijp.DePijpSource;

public interface DePijpMode {
    FlowConnector createFlowConnector();

    <T> Tap createSourceTap(DePijpSource<T> source);

    <T> Tap createSinkTap(DePijpSink<T> sink);
}
