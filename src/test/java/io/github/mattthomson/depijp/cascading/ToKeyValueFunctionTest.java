package io.github.mattthomson.depijp.cascading;

import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.KeyValue;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ToKeyValueFunctionTest {
    @Test
    public void shouldConvertToKeyValue() {
        Fields keyField = new Fields("keyField");
        Fields valueField = new Fields("valueField");
        Fields outputField = new Fields("outputField");
        Function<Void> function = new ToKeyValueFunction(keyField, valueField, outputField);

        TupleEntry argument = new TupleEntry(keyField.append(valueField), new Tuple(1, "foo"));
        ListTupleEntryCollector<KeyValue<Integer, String>> collector = new ListTupleEntryCollector<>(outputField);

        function.operate(null, new ConcreteCall<>(argument, collector));

        assertThat(collector.getValues()).containsExactly(new KeyValue<>(1, "foo"));
    }
}