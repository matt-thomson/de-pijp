package io.github.mattthomson.depijp.tap;

import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tuple.Fields;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;

public class TsvPijpTap extends FilePijpTap<List<String>> {
    public TsvPijpTap(String path) {
        super(path);
    }

    @Override
    protected Scheme<Properties, InputStream, OutputStream, ?, ?> getScheme(Fields field) {
        return new TextDelimited(field, "\t");
    }
}
