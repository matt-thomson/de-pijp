package io.github.mattthomson.depijp;

import io.github.mattthomson.depijp.sink.InMemoryDePijpSink;
import io.github.mattthomson.depijp.source.InMemoryDePijpSource;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupedPijpTest {
    @Test
    public void shouldGroup() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(1, 2, 3);
        InMemoryDePijpSink<KeyValue<Integer, Integer>> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).groupBy(x -> x % 2).toPijp().write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(
                new KeyValue<>(0, 2),
                new KeyValue<>(1, 1),
                new KeyValue<>(1, 3)
        );
    }
}
