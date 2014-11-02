package io.github.mattthomson.depijp.tap;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.github.mattthomson.depijp.PijpSink;
import io.github.mattthomson.depijp.PijpSource;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static io.github.mattthomson.depijp.tap.PijpTapTestUtil.readFromSource;
import static io.github.mattthomson.depijp.tap.PijpTapTestUtil.writeToSink;
import static org.assertj.core.api.Assertions.assertThat;

public class TsvPijpTapTest {
    @Test
    public void shouldReadTabSeparatedValuesFromFile() {
        PijpSource<List<String>> tap = new TsvPijpTap("src/test/resources/values.tsv");
        assertThat(readFromSource(tap)).containsExactly(
                ImmutableList.of("one", "1"),
                ImmutableList.of("two", "2"),
                ImmutableList.of("three", "3")
        );
    }

    @Test
    public void shouldWriteLinesToFile() throws IOException {
        File outputFile = File.createTempFile("output", "txt");
        outputFile.deleteOnExit();

        PijpSink<List<String>> tap = new TsvPijpTap(outputFile.getPath());
        writeToSink(tap,
                ImmutableList.of("one", "1"),
                ImmutableList.of("two", "2"),
                ImmutableList.of("three", "3")
        );

        List<String> result = Files.readLines(outputFile, Charsets.UTF_8);
        assertThat(result).containsExactly("one\t1", "two\t2", "three\t3");
    }

}