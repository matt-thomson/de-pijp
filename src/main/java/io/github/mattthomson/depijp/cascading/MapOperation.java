package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.function.SerializableFunction;

public class MapOperation<T, S> extends BaseOperation<Void> implements Function<Void> {
    private final SerializableFunction<T, S> mapper;
    private final Fields inputField;
    private final Fields outputField;

    public MapOperation(SerializableFunction<T, S> mapper, Fields inputField, Fields outputField) {
        super(1, outputField);

        this.mapper = mapper;
        this.inputField = inputField;
        this.outputField = outputField;
    }

    public MapOperation(SerializableFunction<T, S> mapper, Fields field) {
        this(mapper, field, field);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        T arg = (T) functionCall.getArguments().getObject(inputField);
        functionCall.getOutputCollector().add(new TupleEntry(outputField, new Tuple(mapper.apply(arg))));
    }
}
