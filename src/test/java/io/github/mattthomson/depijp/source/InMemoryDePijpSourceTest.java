package io.github.mattthomson.depijp.source;

import cascading.tap.Tap;
import cascading.tuple.TupleEntryIterator;
import io.github.mattthomson.depijp.DePijpSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class InMemoryDePijpSourceTest {
    @Test
    public void shouldBeAbleToReadFromSource() throws Exception {
        DePijpSource<String> source = new InMemoryDePijpSource<>("one", "two", "three");

        Tap tap = source.createLocalSourceTap();
        TupleEntryIterator iterator = tap.openForRead(null);

        List<String> result = new ArrayList<>();
        iterator.forEachRemaining(entry -> result.add(entry.getString(0)));

        assertThat(result).containsExactly("one", "two", "three");
    }
}