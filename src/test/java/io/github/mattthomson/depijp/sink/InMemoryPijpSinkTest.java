package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.google.common.collect.ImmutableList;
import io.github.mattthomson.depijp.PijpException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryPijpSinkTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldBeAbleToSinkIntoTap() throws Exception {
        Fields field = new Fields("field");
        List<String> values = ImmutableList.of("one", "two", "three");
        InMemoryPijpSink<String> sink = new InMemoryPijpSink<>();

        Tap tap = sink.createSinkTap(field);
        TupleEntryCollector collector = tap.openForWrite(null);
        values.forEach(v -> collector.add(new TupleEntry(field, new Tuple(v))));

        List<String> result = sink.getValues();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void shouldNotBeAbleToSinkTwice() {
        Fields field = new Fields("field");
        InMemoryPijpSink<String> sink = new InMemoryPijpSink<>();

        sink.createSinkTap(field);

        exception.expect(PijpException.class);
        sink.createSinkTap(field);
    }

    @Test
    public void shouldNotBeAbleToGetValuesBeforeSinking() {
        InMemoryPijpSink<String> sink = new InMemoryPijpSink<>();

        exception.expect(PijpException.class);
        sink.getValues();
    }
}