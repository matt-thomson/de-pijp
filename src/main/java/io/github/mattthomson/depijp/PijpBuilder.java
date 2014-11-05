package io.github.mattthomson.depijp;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.cascading.FromTupleEntryFunction;
import io.github.mattthomson.depijp.mode.DePijpMode;
import io.github.mattthomson.depijp.mode.HadoopDePijpMode;
import io.github.mattthomson.depijp.mode.LocalDePijpMode;

import java.util.UUID;

public class PijpBuilder {
    private final FlowDef flowDef = new FlowDef();
    private final DePijpMode mode;

    private PijpBuilder(DePijpMode mode) {
        this.mode = mode;
    }

    public static PijpBuilder local() {
        return new PijpBuilder(new LocalDePijpMode());
    }

    public static PijpBuilder hadoop() {
        return new PijpBuilder(new HadoopDePijpMode());
    }

    public <T> Pijp<T> read(DePijpSource<T> source) {
        Pipe pipe = new Pipe(UUID.randomUUID().toString());
        Fields field = new Fields(UUID.randomUUID().toString());

        flowDef.addSource(pipe, mode.createSourceTap(source));
        Pipe transformed = new Each(pipe, new FromTupleEntryFunction<>(source, field));
        return new Pijp<>(flowDef, mode, transformed, field);
    }

    public void run() {
        Flow flow = mode.createFlowConnector().connect(flowDef);
        flow.complete();
    }
}
