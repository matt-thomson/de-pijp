package io.github.mattthomson.depijp;

import java.util.UUID;
import java.util.stream.Stream;

import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.cascading.FilterOperation;
import io.github.mattthomson.depijp.cascading.FlatMapOperation;
import io.github.mattthomson.depijp.cascading.MapOperation;
import io.github.mattthomson.depijp.cascading.ToTupleEntryFunction;
import io.github.mattthomson.depijp.function.SerializableFunction;
import io.github.mattthomson.depijp.function.SerializablePredicate;
import io.github.mattthomson.depijp.mode.DePijpMode;

import static cascading.tuple.Fields.ALL;
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

    public <R> Pijp<R> map(SerializableFunction<? super T, ? super R> mapper) {
        Pipe transformed = new Each(pipe, field, new MapOperation<>(mapper, field), REPLACE);
        return new Pijp<>(flowDef, mode, transformed, field);
    }

    public <R> Pijp<R> flatMap(SerializableFunction<? super T, ? extends Stream<? extends R>> mapper) {
        Pipe transformed = new Each(pipe, field, new FlatMapOperation<>(mapper, field), REPLACE);
        return new Pijp<>(flowDef, mode, transformed, field);
    }

    public Pijp<T> filter(SerializablePredicate<T> predicate) {
        Pipe transformed = new Each(pipe, field, new FilterOperation<>(predicate, field));
        return new Pijp<>(flowDef, mode, transformed, field);
    }

    public <K> GroupedPijp<K, T> groupBy(SerializableFunction<T, K> classifier) {
        Fields keyField = new Fields(UUID.randomUUID().toString());
        Pipe withKeys = new Each(pipe, field, new MapOperation<>(classifier, field, keyField), ALL);
        GroupBy grouped = new GroupBy(withKeys, keyField);

        return new GroupedPijp<>(flowDef, mode, grouped, keyField, field);
    }

    public void write(DePijpSink<T> sink) {
        Pipe transformed = new Each(pipe, field, new ToTupleEntryFunction<>(sink, field));
        flowDef.addTailSink(transformed, mode.createSinkTap(sink));
    }
}
