package io.github.mattthomson.depijp.mode;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.kryo.KryoSerialization;
import cascading.tap.Tap;
import com.google.common.collect.ImmutableMap;
import io.github.mattthomson.depijp.DePijpSink;
import io.github.mattthomson.depijp.DePijpSource;

public class HadoopDePijpMode implements DePijpMode {
    @Override
    public FlowConnector createFlowConnector() {
        return new Hadoop2MR1FlowConnector(ImmutableMap.of(
                "io.serializations", KryoSerialization.class.getName()
        ));
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
