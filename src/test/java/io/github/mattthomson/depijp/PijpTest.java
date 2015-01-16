package io.github.mattthomson.depijp;

import java.util.stream.Stream;

import com.google.common.collect.Ordering;
import io.github.mattthomson.depijp.sink.InMemoryDePijpSink;
import io.github.mattthomson.depijp.source.InMemoryDePijpSource;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PijpTest {
    @Test
    public void shouldApplyMap() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(1, 2, 3);
        InMemoryDePijpSink<String> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).map(String::valueOf).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly("1", "2", "3");
    }

    @Test
    public void shouldApplyFlatMap() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(1, 2, 3);
        InMemoryDePijpSink<Integer> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).flatMap(i -> Stream.of(i, i * 2)).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(1, 2, 2, 4, 3, 6);
    }

    @Test
    public void shouldApplyFilter() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(1, 2, 3);
        InMemoryDePijpSink<Integer> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).filter(i -> i % 2 == 1).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(1, 3);
    }

    @Test
    public void shouldCrossWithTiny() {
        DePijpSource<Integer> source1 = new InMemoryDePijpSource<>(1, 2, 3);
        DePijpSource<String> source2 = new InMemoryDePijpSource<>("a", "b");
        InMemoryDePijpSink<Pair<Integer, String>> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source1)
                .crossWithTiny(pijpBuilder.read(source2))
                .write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(
                new Pair<>(1, "a"),
                new Pair<>(1, "b"),
                new Pair<>(2, "a"),
                new Pair<>(2, "b"),
                new Pair<>(3, "a"),
                new Pair<>(3, "b")
        );
    }

    @Test
    public void shouldCrossWithTinySelf() {
        DePijpSource<String> source = new InMemoryDePijpSource<>("a", "b");
        InMemoryDePijpSink<Pair<String, String>> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        Pijp<String> pijp = pijpBuilder.read(source);
        pijp.crossWithTiny(pijp).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(
                new Pair<>("a", "a"),
                new Pair<>("a", "b"),
                new Pair<>("b", "a"),
                new Pair<>("b", "b")
        );
    }

    @Test
    public void shouldCross() {
        DePijpSource<Integer> source1 = new InMemoryDePijpSource<>(1, 2, 3);
        DePijpSource<String> source2 = new InMemoryDePijpSource<>("a", "b");
        InMemoryDePijpSink<Pair<Integer, String>> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source1)
                .cross(pijpBuilder.read(source2))
                .write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(
                new Pair<>(1, "a"),
                new Pair<>(1, "b"),
                new Pair<>(2, "a"),
                new Pair<>(2, "b"),
                new Pair<>(3, "a"),
                new Pair<>(3, "b")
        );
    }

    @Test
    public void shouldCrossWithSelf() {
        DePijpSource<String> source = new InMemoryDePijpSource<>("a", "b");
        InMemoryDePijpSink<Pair<String, String>> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        Pijp<String> pijp = pijpBuilder.read(source);
        pijp.cross(pijp).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(
                new Pair<>("a", "a"),
                new Pair<>("a", "b"),
                new Pair<>("b", "a"),
                new Pair<>("b", "b")
        );
    }

    @Test
    public void shouldFindUniques() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(1, 2, 2, 1, 2, 3);
        InMemoryDePijpSink<Integer> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).unique().write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(1, 2, 3);
    }

    @Test
    public void shouldSort() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(2, 4, 3, 1);
        InMemoryDePijpSink<Integer> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).sort(Ordering.<Integer>natural().reverse()).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(4, 3, 2, 1);
    }
}
