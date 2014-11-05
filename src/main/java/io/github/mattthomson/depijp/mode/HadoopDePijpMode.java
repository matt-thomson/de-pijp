package io.github.mattthomson.depijp.mode;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.tap.Tap;
import io.github.mattthomson.depijp.DePijpSink;
import io.github.mattthomson.depijp.DePijpSource;

public class HadoopDePijpMode implements DePijpMode {
    @Override
    public FlowConnector createFlowConnector() {
        return new Hadoop2MR1FlowConnector();
    }

    @Override
    public <T> Tap createSourceTap(DePijpSource<T> source) {
        return source.createHadoopSourceTap();
    }

    @Override
    public <T> Tap createSinkTap(DePijpSink<T> sink) {
        return sink.createHadoopSinkTap();
    }
}
