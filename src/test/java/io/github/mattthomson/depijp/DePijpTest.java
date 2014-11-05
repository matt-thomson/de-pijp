package io.github.mattthomson.depijp;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
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
}