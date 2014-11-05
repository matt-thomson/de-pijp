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

        PijpBuilder pijpBuilder = new PijpBuilder();
        pijpBuilder.read(source).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(1, 2, 3);
    }
}