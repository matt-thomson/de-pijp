package io.github.mattthomson.depijp;

import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;

import cascading.flow.FlowDef;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Unique;
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
import static cascading.tuple.Fields.RESULTS;

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

    public GroupedPijp<T, T> group() {
        return groupBy(x -> x);
    }

    public <K> GroupedPijp<K, T> groupBy(SerializableFunction<T, K> classifier) {
        Fields keyField = new Fields(UUID.randomUUID().toString());
        Fields valueField = new Fields(UUID.randomUUID().toString());
        Pipe withKeys = new Each(pipe, field, new MapOperation<>(classifier, field, keyField), ALL);
        Pipe renamedValues = new Rename(withKeys, field, valueField);

        return new GroupedPijp<>(flowDef, mode, renamedValues, keyField, valueField);
    }

    public <S> Pijp<Pair<T, S>> crossWithTiny(Pijp<S> other) {
        return this.groupBy(x -> 1).joinWithTiny(other.groupBy(x -> 1)).values();
    }

    public <S> Pijp<Pair<T, S>> cross(Pijp<S> other) {
        return this.groupBy(x -> 1).join(other.groupBy(x -> 1)).values();
    }

    public Pijp<T> unique() {
        return new Pijp<>(flowDef, mode, new Unique(pipe, field), field);
    }

    public Pijp<T> sort(Comparator<T> comparator) {
        String fieldName = field.get(0).toString();
        Fields sortField = new Fields(fieldName);
        sortField.setComparator(fieldName, comparator);

        Fields dummyField = new Fields(UUID.randomUUID().toString());
        Pipe withDummy = new Each(pipe, new Insert(dummyField, 1), ALL);
        Pipe sorted = new GroupBy(withDummy, dummyField, sortField);
        Pipe withoutDummy = new Each(sorted, sortField, new Identity(), RESULTS);

        return new Pijp<>(flowDef, mode, withoutDummy, sortField);
    }

    public void write(DePijpSink<T> sink) {
        Pipe transformed = new Each(pipe, field, new ToTupleEntryFunction<>(sink, field));
        Pipe renamed = new Pipe(UUID.randomUUID().toString(), transformed);
        flowDef.addTailSink(renamed, mode.createSinkTap(sink));
    }
}
