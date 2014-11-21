package io.github.mattthomson.depijp;

import java.util.function.Function;

import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.cascading.MapFunction;
import io.github.mattthomson.depijp.cascading.ToTupleEntryFunction;
import io.github.mattthomson.depijp.mode.DePijpMode;

import static cascading.tuple.Fields.REPLACE;

public class Pijp<T> {
    private final FlowDef flowDef;
    private final DePijpMode mode;
    private final Pipe pipe;
    private final Fields field;

    public Pijp(FlowDef flowDef, DePijpMode mode, Pipe pipe, Fields field) {
        this.flowDef = flowDef;
        this.mode = mode;
        this.pipe = pipe;
        this.field = field;
    }

    public <S> Pijp<S> map(Function<T, S> function) {
        Pipe transformed = new Each(pipe, new MapFunction<>(function, field), REPLACE);
        return new Pijp<>(flowDef, mode, transformed, field);
    }

    public void write(DePijpSink<T> sink) {
        Pipe transformed = new Each(pipe, new ToTupleEntryFunction<>(sink, field));
        flowDef.addTailSink(transformed, mode.createSinkTap(sink));
    }
}
