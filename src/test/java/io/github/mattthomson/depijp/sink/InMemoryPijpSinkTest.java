package io.github.mattthomson.depijp.sink;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryPijpSinkTest {
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
}