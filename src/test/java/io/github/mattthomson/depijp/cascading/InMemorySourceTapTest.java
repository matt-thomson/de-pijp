package io.github.mattthomson.depijp.cascading;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemorySourceTapTest {
    @Test
    public void shouldBeAbleToSourceFromTap() throws Exception {
        Fields field = new Fields("field");
        List<String> values = ImmutableList.of("one", "two", "three");
        Tap tap = new InMemorySourceTap<>(field, values);

        TupleEntryIterator iterator = tap.openForRead(null);
        List<String> result = new ArrayList<>();
        iterator.forEachRemaining(e -> result.add(e.getString(field)));

        assertThat(result).isEqualTo(values);
    }
}