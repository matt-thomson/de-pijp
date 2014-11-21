package io.github.mattthomson.depijp;

import java.util.UUID;

import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.cascading.ReduceOperation;
import io.github.mattthomson.depijp.cascading.ToKeyValueFunction;
import io.github.mattthomson.depijp.function.SerializableBiFunction;
import io.github.mattthomson.depijp.mode.DePijpMode;

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

    public Pijp<KeyValue<K, V>> toPijp() {
        return toKeyValue(pipe);
    }

    private <T> Pijp<KeyValue<K, T>> toKeyValue(Pipe pipe) {
        Fields outputField = new Fields(UUID.randomUUID().toString());
        Pipe transformed = new Each(pipe, keyField.append(valueField), new ToKeyValueFunction<>(keyField, valueField, outputField));
        return new Pijp<>(flowDef, mode, transformed, outputField);
    }
}
