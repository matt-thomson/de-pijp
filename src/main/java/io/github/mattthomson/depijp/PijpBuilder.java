package io.github.mattthomson.depijp;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;

public class PijpBuilder {
    private final FlowDef flowDef = new FlowDef();

    public <T> Pijp<T> read(PijpSource<T> source) {
        return source.readFrom(flowDef);
    }

    public void run() {
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}
