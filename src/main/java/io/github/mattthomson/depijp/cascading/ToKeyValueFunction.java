package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.KeyValue;

public class ToKeyValueFunction<K, V> extends BaseOperation<Void> implements Function<Void> {
    private final Fields keyField;
    private final Fields valueField;
    private final Fields outputField;

    public ToKeyValueFunction(Fields keyField, Fields valueField, Fields outputField) {
        super(2, outputField);

        this.keyField = keyField;
        this.valueField = valueField;
        this.outputField = outputField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        K key = (K) functionCall.getArguments().getObject(keyField);
        V value = (V) functionCall.getArguments().getObject(valueField);

        functionCall.getOutputCollector().add(new TupleEntry(outputField, new Tuple(new KeyValue<>(key, value))));
    }
}
