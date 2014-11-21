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
    private final Fields field;

    public MapOperation(SerializableFunction<T, S> mapper, Fields field) {
        super(field);

        this.mapper = mapper;
        this.field = field;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        T arg = (T) functionCall.getArguments().getObject(field);
        functionCall.getOutputCollector().add(new TupleEntry(field, new Tuple(mapper.apply(arg))));
    }
}
