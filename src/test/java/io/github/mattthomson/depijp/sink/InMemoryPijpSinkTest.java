package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Tuple;
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
        List<String> values = ImmutableList.of("one", "two", "three");
        InMemoryPijpSink<String> sink = new InMemoryPijpSink<>();

        Tap tap = sink.createSinkTap();
        TupleEntryCollector collector = tap.openForWrite(null);
        values.forEach(v -> collector.add(new Tuple(v)));

        List<String> result = sink.getValues();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void shouldNotBeAbleToSinkTwice() {
        InMemoryPijpSink<String> sink = new InMemoryPijpSink<>();

        sink.createSinkTap();

        exception.expect(PijpException.class);
        sink.createSinkTap();
    }

    @Test
    public void shouldNotBeAbleToGetValuesBeforeSinking() {
        InMemoryPijpSink<String> sink = new InMemoryPijpSink<>();

        exception.expect(PijpException.class);
        sink.getValues();
    }
}