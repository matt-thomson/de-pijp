package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.DePijpSink;

public class ToTupleEntryFunction<T> extends BaseOperation<Void> implements Function<Void> {
    private final DePijpSink<T> sink;
    private final Fields inputField;

    public ToTupleEntryFunction(DePijpSink<T> sink, Fields inputField) {
        super(1, sink.getOutputFields());

        this.sink = sink;
        this.inputField = inputField;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        T value = (T) functionCall.getArguments().getObject(inputField);
        functionCall.getOutputCollector().add(new TupleEntry(sink.getOutputFields(), sink.toTuple(value)));
    }
}
