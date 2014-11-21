package io.github.mattthomson.depijp.cascading;

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.function.SerializableBiFunction;

public class ReduceOperation<T, V> extends BaseOperation<Void> implements Buffer<Void> {
    private final T initialValue;
    private final SerializableBiFunction<T, V, T> op;
    private final Fields valueField;

    public ReduceOperation(T initialValue, SerializableBiFunction<T, V, T> op, Fields valueField) {
        super(1, valueField);

        this.initialValue = initialValue;
        this.op = op;
        this.valueField = valueField;
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall<Void> bufferCall) {
        T value = initialValue;
        Iterator<TupleEntry> argumentsIterator = bufferCall.getArgumentsIterator();

        while (argumentsIterator.hasNext()) {
            V arg = (V) argumentsIterator.next().getObject(valueField);
            value = op.apply(value, arg);
        }

        bufferCall.getOutputCollector().add(new TupleEntry(valueField, new Tuple(value)));
    }
}
