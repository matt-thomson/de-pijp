package io.github.mattthomson.depijp.source;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryPijpSourceTest {
    @Test
    public void shouldBeAbleToReadFromTap() throws Exception {
        PijpSource<String> source = new InMemoryPijpSource<>("one", "two", "three");
        Fields field = new Fields("field");

        Tap tap = source.createSourceTap(field);
        TupleEntryIterator iterator = tap.openForRead(null);

        List<String> result = new ArrayList<>();
        iterator.forEachRemaining(entry -> result.add(entry.getString(field)));

        assertThat(result).containsExactly("one", "two", "three");
    }
}