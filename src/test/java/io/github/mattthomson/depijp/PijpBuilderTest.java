package io.github.mattthomson.depijp;

import io.github.mattthomson.depijp.sink.InMemoryPijpSink;
import io.github.mattthomson.depijp.source.InMemoryPijpSource;
import io.github.mattthomson.depijp.source.PijpSource;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PijpBuilderTest {
    @Test
    public void shouldPassThrough() {
        PijpSource<Integer> source = new InMemoryPijpSource<>(1, 2, 3);
        InMemoryPijpSink<Integer> sink = new InMemoryPijpSink<>();

        PijpBuilder pijpBuilder = new PijpBuilder();
        pijpBuilder.read(source).write(sink);
        pijpBuilder.run();

        assertThat(sink.getValues()).containsExactly(1, 2, 3);
    }
}