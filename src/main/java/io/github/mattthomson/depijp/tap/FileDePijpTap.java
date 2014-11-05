package io.github.mattthomson.depijp.tap;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import io.github.mattthomson.depijp.DePijpTap;

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
    public Tap createTap() {
        return new FileTap(getScheme(), path, REPLACE);
    }

    protected abstract Scheme<Properties, InputStream, OutputStream, ?, ?> getScheme();
}
