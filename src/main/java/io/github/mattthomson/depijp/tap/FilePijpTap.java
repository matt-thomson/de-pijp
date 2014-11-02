package io.github.mattthomson.depijp.tap;

import cascading.scheme.Scheme;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import io.github.mattthomson.depijp.PijpTap;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import static cascading.tap.SinkMode.REPLACE;

public abstract class FilePijpTap<T> implements PijpTap<T> {
    protected final String path;

    public FilePijpTap(String path) {
        this.path = path;
    }

    @Override
    public Tap createTap(Fields field) {
        return new FileTap(getScheme(field), path, REPLACE);
    }

    protected abstract Scheme<Properties, InputStream, OutputStream, ?, ?> getScheme(Fields field);
}
