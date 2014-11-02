package io.github.mattthomson.depijp.cascading;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ListTupleEntryIteratorTest {
    @Test
    public void shouldConvertListToTupleEntries() {
        Fields field = new Fields("field");
        List<String> values = ImmutableList.of("one", "two", "three");
        TupleEntryIterator iterator = new ListTupleEntryIterator<>(field, values);

        List<String> result = new ArrayList<>();
        iterator.forEachRemaining(e -> result.add(e.getString(field)));

        assertThat(result).isEqualTo(values);
    }
}