package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.PijpSource;

public class FromTupleEntryFunction<T> extends BaseOperation<Void> implements Function<Void> {
    private final PijpSource<T> source;

    public FromTupleEntryFunction(PijpSource<T> source, Fields field) {
        super(field);

        this.source = source;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
        T transformed = source.fromTupleEntry(tupleEntry);

        functionCall.getOutputCollector().add(new Tuple(transformed));
    }
}
