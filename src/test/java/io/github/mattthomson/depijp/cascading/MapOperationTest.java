package io.github.mattthomson.depijp.cascading;

import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MapOperationTest {
    @Test
    public void shouldApplyMap() {
        Fields field = new Fields("field");
        Function<Void> function = new MapOperation<>(String::valueOf, field);

        TupleEntry argument = new TupleEntry(field, new Tuple(1));
        ListTupleEntryCollector<String> collector = new ListTupleEntryCollector<>(field);

        function.operate(null, new ConcreteCall<>(argument, collector));

        assertThat(collector.getValues()).containsExactly("1");
    }

    @Test
    public void shouldApplyMapWithOutputField() {
        Fields inputField = new Fields("inputField");
        Fields outputField = new Fields("outputField");

        Function<Void> function = new MapOperation<>(String::valueOf, inputField, outputField);

        TupleEntry argument = new TupleEntry(inputField, new Tuple(1));
        ListTupleEntryCollector<String> collector = new ListTupleEntryCollector<>(outputField);

        function.operate(null, new ConcreteCall<>(argument, collector));

        assertThat(collector.getValues()).containsExactly("1");
    }
}