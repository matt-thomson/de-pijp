package io.github.mattthomson.depijp.cascading;

import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.DePijpSource;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FromTupleEntryFunctionTest {
    @Test
    public void shouldExtractValueFromTupleEntry() {
        DePijpSource<String> source = mock(DePijpSource.class);
        Fields field = new Fields("field");
        Function<Void> function = new FromTupleEntryFunction<>(source, field);

        TupleEntry argument = new TupleEntry(field, new Tuple("foo"));
        ListTupleEntryCollector<String> collector = new ListTupleEntryCollector<>(field);

        when(source.fromTupleEntry(argument)).thenReturn("bar");

        function.operate(null, new ConcreteCall<>(argument, collector));

        assertThat(collector.getValues()).containsExactly("bar");
    }
}