package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class MapFunction<T, S> extends BaseOperation<Void> implements Function<Void> {
    private final java.util.function.Function<T, S> function;
    private final Fields field;

    public MapFunction(java.util.function.Function<T, S> function, Fields field) {
        super(field);

        this.function = function;
        this.field = field;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        T arg = (T) functionCall.getArguments().getObject(field);
        functionCall.getOutputCollector().add(new TupleEntry(field, new Tuple(function.apply(arg))));
    }
}
