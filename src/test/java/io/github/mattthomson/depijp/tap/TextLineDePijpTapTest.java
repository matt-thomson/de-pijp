package io.github.mattthomson.depijp.tap;

import com.google.common.base.Charsets;
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

public class TextLineDePijpTapTest {
    @Test
    public void shouldReadLinesFromFile() {
        DePijpSource<String> tap = new TextLineDePijpTap("src/test/resources/values.txt");
        assertThat(readFromSource(tap)).containsExactly("one", "two", "three");
    }

    @Test
    public void shouldWriteLinesToFile() throws IOException {
        File outputFile = File.createTempFile("output", "txt");
        outputFile.deleteOnExit();

        DePijpSink<String> tap = new TextLineDePijpTap(outputFile.getPath());
        writeToSink(tap, "one", "two", "three");

        List<String> result = Files.readLines(outputFile, Charsets.UTF_8);
        assertThat(result).containsExactly("one", "two", "three");
    }
}