package io.github.mattthomson.depijp.source;

import cascading.tap.Tap;
import cascading.tuple.TupleEntryIterator;
import io.github.mattthomson.depijp.PijpSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryPijpSourceTest {
    @Test
    public void shouldBeAbleToReadFromSource() throws Exception {
        PijpSource<String> source = new InMemoryPijpSource<>("one", "two", "three");

        Tap tap = source.createSourceTap();
        TupleEntryIterator iterator = tap.openForRead(null);

        List<String> result = new ArrayList<>();
        iterator.forEachRemaining(entry -> result.add(entry.getString(0)));

        assertThat(result).containsExactly("one", "two", "three");
    }
}