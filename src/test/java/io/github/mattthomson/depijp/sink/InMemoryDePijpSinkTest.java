package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import com.google.common.collect.ImmutableList;
import io.github.mattthomson.depijp.DePijpException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryDePijpSinkTest {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldBeAbleToSinkIntoTap() throws Exception {
        List<String> values = ImmutableList.of("one", "two", "three");
        InMemoryDePijpSink<String> sink = new InMemoryDePijpSink<>();

        Tap tap = sink.createLocalSinkTap();
        TupleEntryCollector collector = tap.openForWrite(null);
        values.forEach(v -> collector.add(new Tuple(v)));

        List<String> result = sink.getValues();
        assertThat(result).isEqualTo(values);
    }

    @Test
    public void shouldNotBeAbleToSinkTwice() {
        InMemoryDePijpSink<String> sink = new InMemoryDePijpSink<>();

        sink.createLocalSinkTap();

        exception.expect(DePijpException.class);
        sink.createLocalSinkTap();
    }

    @Test
    public void shouldNotBeAbleToGetValuesBeforeSinking() {
        InMemoryDePijpSink<String> sink = new InMemoryDePijpSink<>();

        exception.expect(DePijpException.class);
        sink.getValues();
    }
}