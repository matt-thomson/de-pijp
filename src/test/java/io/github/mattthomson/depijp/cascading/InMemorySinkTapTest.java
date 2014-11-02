package io.github.mattthomson.depijp.cascading;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemorySinkTapTest {
    @Test
    public void shouldBeAbleToSinkIntoTap() throws Exception {
        Fields field = new Fields("field");
        InMemorySinkTap<String> tap = new InMemorySinkTap<>(field);

        TupleEntryCollector collector = tap.openForWrite(null);
        List<String> values = ImmutableList.of("one", "two", "three");
        values.forEach(v -> collector.add(new TupleEntry(field, new Tuple(v))));

        assertThat(tap.getValues()).isEqualTo(values);
    }
}
