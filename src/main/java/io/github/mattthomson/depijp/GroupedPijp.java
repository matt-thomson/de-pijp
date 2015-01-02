package io.github.mattthomson.depijp;

import cascading.flow.FlowDef;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.cascading.ReduceOperation;
import io.github.mattthomson.depijp.cascading.ToKeyValueFunction;
import io.github.mattthomson.depijp.cascading.ToPairFunction;
import io.github.mattthomson.depijp.function.SerializableBiFunction;
import io.github.mattthomson.depijp.mode.DePijpMode;

import java.util.UUID;

import static cascading.tuple.Fields.REPLACE;

public class GroupedPijp<K, V> {
    private final FlowDef flowDef;
    private final DePijpMode mode;
    private final Pipe pipe;
    private final Fields keyField;
    private final Fields valueField;

    public GroupedPijp(FlowDef flowDef, DePijpMode mode, Pipe pipe, Fields keyField, Fields valueField) {
        this.flowDef = flowDef;
        this.mode = mode;
        this.pipe = pipe;
        this.keyField = keyField;
        this.valueField = valueField;
    }

    public <T> Pijp<KeyValue<K, T>> reduce(T initialValue, SerializableBiFunction<T, V, T> op) {
        Pipe reduced = new Every(pipe, valueField, new ReduceOperation<>(initialValue, op, valueField), REPLACE);
        return toKeyValue(reduced);
    }

    public Pijp<KeyValue<K, Integer>> count() {
        return reduce(0, (count, next) -> count + 1);
    }

    public Pijp<V> values() {
        Pipe values = new Each(pipe, valueField, new Identity());
        return new Pijp<>(flowDef, mode, values, valueField);
    }

    public <W> GroupedPijp<K, Pair<V, W>> hashJoin(GroupedPijp<K, W> other) {
        Fields outputField = new Fields(UUID.randomUUID().toString());
        Pipe joined = new HashJoin(pipe, keyField, other.getPipe(), other.getKeyField(), new InnerJoin());
        Pipe transformed = new Each(joined, valueField.append(other.getValueField()), new ToPairFunction<>(valueField, other.getValueField(), outputField));
        return new GroupedPijp<>(flowDef, mode, transformed, keyField, outputField);
    }

    public Pijp<KeyValue<K, V>> toPijp() {
        return toKeyValue(pipe);
    }

    private <T> Pijp<KeyValue<K, T>> toKeyValue(Pipe pipe) {
        Fields outputField = new Fields(UUID.randomUUID().toString());
        Pipe transformed = new Each(pipe, keyField.append(valueField), new ToKeyValueFunction<>(keyField, valueField, outputField));
        return new Pijp<>(flowDef, mode, transformed, outputField);
    }

    Pipe getPipe() {
        return pipe;
    }

    Fields getKeyField() {
        return keyField;
    }

    Fields getValueField() {
        return valueField;
    }
}
