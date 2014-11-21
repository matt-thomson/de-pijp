package io.github.mattthomson.depijp;

import java.util.UUID;

import cascading.flow.FlowDef;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.cascading.ToKeyValueFunction;
import io.github.mattthomson.depijp.mode.DePijpMode;

public class GroupedPijp<K, V> {
    private final FlowDef flowDef;
    private final DePijpMode mode;
    private final GroupBy pipe;
    private final Fields keyField;
    private final Fields valueField;

    public GroupedPijp(FlowDef flowDef, DePijpMode mode, GroupBy pipe, Fields keyField, Fields valueField) {
        this.flowDef = flowDef;
        this.mode = mode;
        this.pipe = pipe;
        this.keyField = keyField;
        this.valueField = valueField;
    }

    public Pijp<KeyValue<K, V>> toPijp() {
        Fields outputField = new Fields(UUID.randomUUID().toString());
        Pipe transformed = new Each(pipe, keyField.append(valueField), new ToKeyValueFunction<>(keyField, valueField, outputField));
        return new Pijp<>(flowDef, mode, transformed, outputField);
    }
}
