package io.github.mattthomson.depijp.tap;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.github.mattthomson.depijp.DePijpSink;
import io.github.mattthomson.depijp.DePijpSource;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static io.github.mattthomson.depijp.tap.DePijpTapTestUtil.readFromSource;
import static io.github.mattthomson.depijp.tap.DePijpTapTestUtil.writeToSink;
import static org.assertj.core.api.Assertions.assertThat;

public class TsvDePijpTapTest {
    @Test
    public void shouldReadTabSeparatedValuesFromFile() {
        DePijpSource<List<String>> tap = new TsvDePijpTap("src/test/resources/values.tsv", 2);
        assertThat(readFromSource(tap)).containsExactly(
                ImmutableList.of("one", "1"),
                ImmutableList.of("two", "2"),
                ImmutableList.of("three", "3")
        );
    }

    @Test
    public void shouldWriteLinesToFile() throws IOException {
        File outputFile = File.createTempFile("output", "tsv");
        outputFile.deleteOnExit();

        DePijpSink<List<String>> tap = new TsvDePijpTap(outputFile.getPath(), 2);
        writeToSink(tap,
                ImmutableList.of("one", "1"),
                ImmutableList.of("two", "2"),
                ImmutableList.of("three", "3")
        );

        List<String> result = Files.readLines(outputFile, Charsets.UTF_8);
        assertThat(result).containsExactly("one\t1", "two\t2", "three\t3");
    }
}