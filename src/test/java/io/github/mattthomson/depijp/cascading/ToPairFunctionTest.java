package io.github.mattthomson.depijp.cascading;

import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.Pair;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ToPairFunctionTest {
    @Test
    public void shouldConvertToPair() {
        Fields firstField = new Fields("firstField");
        Fields secondField = new Fields("secondField");
        Fields outputField = new Fields("outputField");
        Function<Void> function = new ToPairFunction<Integer, String>(firstField, secondField, outputField);

        TupleEntry argument = new TupleEntry(firstField.append(secondField), new Tuple(1, "foo"));
        ListTupleEntryCollector<Pair<Integer, String>> collector = new ListTupleEntryCollector<>(outputField);

        function.operate(null, new ConcreteCall<>(argument, collector));

        assertThat(collector.getValues()).containsExactly(new Pair<>(1, "foo"));
    }
}