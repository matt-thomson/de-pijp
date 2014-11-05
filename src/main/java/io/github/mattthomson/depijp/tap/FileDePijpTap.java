package io.github.mattthomson.depijp.tap;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import io.github.mattthomson.depijp.DePijpTap;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import static cascading.tap.SinkMode.REPLACE;

public abstract class FileDePijpTap<T> implements DePijpTap<T> {
    protected final String path;

    public FileDePijpTap(String path) {
        this.path = path;
    }

    @Override
    public Tap createLocalTap() {
        return new FileTap(getLocalScheme(), path, REPLACE);
    }

    @Override
    public Tap createHadoopTap() {
        return new Hfs(getHadoopScheme(), path, REPLACE);
    }

    protected abstract Scheme<Properties, InputStream, OutputStream, ?, ?> getLocalScheme();

    protected abstract Scheme<JobConf, RecordReader, OutputCollector, ?, ?> getHadoopScheme();
}
