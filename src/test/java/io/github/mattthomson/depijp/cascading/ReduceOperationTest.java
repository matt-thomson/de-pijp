package io.github.mattthomson.depijp.cascading;

import java.util.List;

import cascading.operation.Buffer;
import cascading.operation.ConcreteCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ReduceOperationTest {
    @Test
    public void shouldConvertToKeyValue() {
        Fields field = new Fields("field");
        Buffer<Void> buffer = new ReduceOperation<String, Integer>("", (x, y) -> x + String.valueOf(y), field);

        ListTupleEntryCollector<String> collector = new ListTupleEntryCollector<>(field);
        List<TupleEntry> arguments = Lists.newArrayList(
                new TupleEntry(field, new Tuple(1)),
                new TupleEntry(field, new Tuple(2)),
                new TupleEntry(field, new Tuple(3))
        );

        final ConcreteCall<Void> functionCall = new ConcreteCall<>();
        functionCall.setArgumentsIterator(arguments.iterator());
        functionCall.setOutputCollector(collector);

        buffer.operate(null, functionCall);

        assertThat(collector.getValues()).containsExactly("123");
    }
}