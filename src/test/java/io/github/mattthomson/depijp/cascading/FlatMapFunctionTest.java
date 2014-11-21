package io.github.mattthomson.depijp.cascading;

import java.util.stream.Stream;

import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FlatMapFunctionTest {
    @Test
    public void shouldApplyMap() {
        Fields field = new Fields("field");
        Function<Void> function = new FlatMapFunction<Integer, Integer>(i -> Stream.of(i, i * 2), field);

        TupleEntry argument = new TupleEntry(field, new Tuple(1));
        ListTupleEntryCollector<Integer> collector = new ListTupleEntryCollector<>(field);

        function.operate(null, new ConcreteCall<>(argument, collector));

        assertThat(collector.getValues()).containsExactly(1, 2);
    }
}