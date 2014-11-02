package io.github.mattthomson.depijp.cascading;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ListTupleEntryCollectorTest {
    @Test
    public void shouldBuildListFromTupleEntries() {
        Fields field = new Fields("field");
        ListTupleEntryCollector<String> collector = new ListTupleEntryCollector<>(field);

        List<String> values = ImmutableList.of("one", "two", "three");
        values.forEach(v -> collector.add(new Tuple(v)));

        assertThat(collector.getValues()).isEqualTo(values);
    }
}