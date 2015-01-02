package io.github.mattthomson.depijp;

import io.github.mattthomson.depijp.sink.InMemoryDePijpSink;
import io.github.mattthomson.depijp.source.InMemoryDePijpSource;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupedPijpTest {
    @Test
    public void shouldGroupByFunction() {
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

    @Test
    public void shouldReduce() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(1, 2, 3);
        InMemoryDePijpSink<KeyValue<Integer, Integer>> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).groupBy(x -> x % 2).reduce(0, (x, y) -> x + y).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(
                new KeyValue<>(0, 2),
                new KeyValue<>(1, 4)
        );
    }

    @Test
    public void shouldGroupAndCount() {
        DePijpSource<String> source = new InMemoryDePijpSource<>("a", "b", "a", "a");
        InMemoryDePijpSink<KeyValue<String, Integer>> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).group().count().write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(
                new KeyValue<>("a", 3),
                new KeyValue<>("b", 1)
        );
    }

    @Test
    public void shouldHashJoin() {
        DePijpSource<String> source1 = new InMemoryDePijpSource<>("0", "1", "2");
        DePijpSource<Integer> source2 = new InMemoryDePijpSource<>(1, 2, 3);
        InMemoryDePijpSink<Pair<String, Integer>> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        GroupedPijp<Integer, String> group1 = pijpBuilder.read(source1).groupBy(Integer::parseInt);
        GroupedPijp<Integer, Integer> group2 = pijpBuilder.read(source2).groupBy(x -> x % 2);
        group1.hashJoin(group2).values().write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(
                new Pair<>("0", 2),
                new Pair<>("1", 1),
                new Pair<>("1", 3)
        );
    }
}
