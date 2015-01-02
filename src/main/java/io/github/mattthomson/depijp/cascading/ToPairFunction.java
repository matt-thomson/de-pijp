package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.KeyValue;
import io.github.mattthomson.depijp.Pair;

public class ToPairFunction<S, T> extends BaseOperation<Void> implements Function<Void> {
    private final Fields firstField;
    private final Fields secondField;
    private final Fields outputField;

    public ToPairFunction(Fields firstField, Fields secondField, Fields outputField) {
        super(2, outputField);

        this.firstField = firstField;
        this.secondField = secondField;
        this.outputField = outputField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        S first = (S) functionCall.getArguments().getObject(firstField);
        T second = (T) functionCall.getArguments().getObject(secondField);

        functionCall.getOutputCollector().add(new TupleEntry(outputField, new Tuple(new Pair<>(first, second))));
    }
}
