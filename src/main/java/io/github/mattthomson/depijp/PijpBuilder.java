package io.github.mattthomson.depijp;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.source.PijpSource;

import java.util.UUID;

public class PijpBuilder {
    private final FlowDef flowDef = new FlowDef();

    public <T> Pijp<T> read(PijpSource<T> source) {
        Pipe pipe = new Pipe(UUID.randomUUID().toString());
        Fields field = new Fields(UUID.randomUUID().toString());

        flowDef.addSource(pipe, source.createSourceTap(field));
        return new Pijp<>(flowDef, pipe, field);
    }

    public void run() {
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}
