package io.github.mattthomson.depijp;

import io.github.mattthomson.depijp.sink.InMemoryDePijpSink;
import io.github.mattthomson.depijp.source.InMemoryDePijpSource;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PijpBuilderTest {
    @Test
    public void shouldPassThrough() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(1, 2, 3);
        InMemoryDePijpSink<Integer> sink = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        pijpBuilder.read(source).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(1, 2, 3);
    }

    @Test
    public void shouldPassThroughToMultipleSinks() {
        DePijpSource<Integer> source = new InMemoryDePijpSource<>(1, 2, 3);
        InMemoryDePijpSink<Integer> sink1 = new InMemoryDePijpSink<>();
        InMemoryDePijpSink<Integer> sink2 = new InMemoryDePijpSink<>();

        PijpBuilder pijpBuilder = PijpBuilder.local();
        Pijp<Integer> pijp = pijpBuilder.read(source);
        pijp.write(sink1);
        pijp.write(sink2);
        pijpBuilder.run();

        assertThat(sink1.getValues()).containsExactly(1, 2, 3);
        assertThat(sink2.getValues()).containsExactly(1, 2, 3);
    }
}
