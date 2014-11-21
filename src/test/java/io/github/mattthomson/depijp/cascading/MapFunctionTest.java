package io.github.mattthomson.depijp.cascading;

import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MapFunctionTest {
    @Test
    public void shouldApplyMap() {
        Fields field = new Fields("field");
        Function<Void> function = new MapFunction<>(String::valueOf, field);

        TupleEntry argument = new TupleEntry(field, new Tuple(1));
        ListTupleEntryCollector<String> collector = new ListTupleEntryCollector<>(field);

        function.operate(null, new ConcreteCall<>(argument, collector));

        assertThat(collector.getValues()).containsExactly("1");
    }
}