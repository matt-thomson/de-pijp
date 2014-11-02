package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.io.IOException;
import java.util.Properties;

class InMemoryScheme extends Scheme<Properties, Void, Void, Void, Void> {
    public InMemoryScheme(Fields field) {
        setSourceFields(field);
        setSinkFields(field);
    }

    @Override
    public void sourceConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, Void, Void> tap, Properties conf) {
    }

    @Override
    public void sinkConfInit(FlowProcess<Properties> flowProcess, Tap<Properties, Void, Void> tap, Properties conf) {
    }

    @Override
    public boolean source(FlowProcess<Properties> flowProcess, SourceCall<Void, Void> sourceCall) throws IOException {
        return false;
    }

    @Override
    public void sink(FlowProcess<Properties> flowProcess, SinkCall<Void, Void> sinkCall) throws IOException {
    }
}
