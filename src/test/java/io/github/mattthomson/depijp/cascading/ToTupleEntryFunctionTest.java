package io.github.mattthomson.depijp.cascading;

import cascading.operation.ConcreteCall;
import cascading.operation.Function;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import io.github.mattthomson.depijp.DePijpSink;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ToTupleEntryFunctionTest {
    @Test
    public void shouldWrapValueInTupleEntry() {
        Fields inputField = new Fields("inputField");
        Fields outputField = new Fields("outputField");

        DePijpSink<String> sink = mock(DePijpSink.class);
        when(sink.getOutputFields()).thenReturn(new Fields("outputField"));

        Function<Void> function = new ToTupleEntryFunction<>(sink, inputField);

        TupleEntry argument = new TupleEntry(inputField, new Tuple("foo"));
        ListTupleEntryCollector<String> collector = new ListTupleEntryCollector<>(outputField);

        when(sink.toTuple("foo")).thenReturn(new Tuple("bar"));

        function.operate(null, new ConcreteCall<>(argument, collector));

        assertThat(collector.getValues()).containsExactly("bar");
    }
}