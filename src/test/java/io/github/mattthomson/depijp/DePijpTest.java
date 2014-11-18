package io.github.mattthomson.depijp;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import io.github.mattthomson.depijp.tap.TextLineDePijpTap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DePijpTest {
    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Test
    public void shouldRunFlow() throws Exception {
        File outputFile = File.createTempFile("output", "txt");
        outputFile.deleteOnExit();

        exit.expectSystemExitWithStatus(0);
        DePijp.main(new String[]{SimpleFlow.class.getName(), outputFile.getPath()});

        List<String> result = Files.readLines(outputFile, Charsets.UTF_8);
        assertThat(result).containsExactly("one", "two", "three");
    }

    @Test
    public void shouldReturnNonZeroExitCodeIfFlowFails() throws Exception {
        File outputFile = File.createTempFile("output", "txt");
        outputFile.deleteOnExit();

        exit.expectSystemExitWithStatus(1);
        DePijp.main(new String[]{InvalidFlow.class.getName(), outputFile.getPath()});
    }

    public static class SimpleFlow implements DePijpFlow {
        @Override
        public void flow(PijpBuilder pijpBuilder, String[] args) {
            pijpBuilder
                    .read(new TextLineDePijpTap("src/test/resources/values.txt"))
                    .write(new TextLineDePijpTap(args[1]));
        }
    }

    public static class InvalidFlow implements DePijpFlow {
        @Override
        public void flow(PijpBuilder pijpBuilder, String[] args) {
            pijpBuilder
                    .read(new TextLineDePijpTap("src/test/resources/values.txt"));
        }
    }
}