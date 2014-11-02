package io.github.mattthomson.depijp.tap;

import com.google.common.base.Charsets;
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

public class TextLinePijpTapTest {
    @Test
    public void shouldReadLinesFromFile() {
        PijpSource<String> tap = new TextLinePijpTap("src/test/resources/lines.txt");
        assertThat(readFromSource(tap)).containsExactly("one", "two", "three");
    }

    @Test
    public void shouldWriteLinesToFile() throws IOException {
        File outputFile = File.createTempFile("output", "txt");
        outputFile.deleteOnExit();

        PijpSink<String> tap = new TextLinePijpTap(outputFile.getPath());
        writeToSink(tap, "one", "two", "three");

        List<String> result = Files.readLines(outputFile, Charsets.UTF_8);
        assertThat(result).containsExactly("one", "two", "three");
    }
}