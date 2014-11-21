package io.github.mattthomson.depijp.cascading;

import java.util.stream.Stream;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.function.SerializableFunction;

public class FlatMapFunction<T, R> extends BaseOperation<Void> implements Function<Void> {
    private final SerializableFunction<? super T, ? extends Stream<? extends R>> mapper;
    private final Fields field;

    public FlatMapFunction(SerializableFunction<? super T, ? extends Stream<? extends R>> mapper, Fields field) {
        super(field);

        this.mapper = mapper;
        this.field = field;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
        T arg = (T) functionCall.getArguments().getObject(field);
        Stream<? extends R> results = mapper.apply(arg);
        results.forEach(result -> functionCall.getOutputCollector().add(new TupleEntry(field, new Tuple(result))));
    }
}
