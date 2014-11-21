package io.github.mattthomson.depijp.cascading;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.function.SerializablePredicate;

public class FilterOperation<T> extends BaseOperation<Void> implements Filter<Void> {
    private final SerializablePredicate<T> predicate;
    private final Fields field;

    public FilterOperation(SerializablePredicate<T> predicate, Fields field) {
        super(1);

        this.predicate = predicate;
        this.field = field;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall<Void> filterCall) {
        T arg = (T) filterCall.getArguments().getObject(field);
        return !predicate.test(arg);
    }
}
